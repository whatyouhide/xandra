defmodule Xandra.Cluster.Pool do
  @moduledoc false

  # This module is the internal process governing the cluster itself. Xandra.Cluster
  # is just the shell that contains documentation and public API, but it's a big shell
  # and this implementation is complex, so splitting them up will make it easier
  # to work on the internals. Plus, the surface API of *this* internal module is
  # quite small, so it's really mostly internals.

  use GenServer

  alias Xandra.Cluster.{Host, LoadBalancingPolicy}

  ## Public API

  @spec start_link(keyword(), keyword()) :: GenServer.on_start()
  def start_link(cluster_opts, pool_opts) do
    {sync_connect_timeout, cluster_opts} = Keyword.pop!(cluster_opts, :sync_connect)

    alias_or_nil = if sync_connect_timeout, do: alias()

    result =
      GenServer.start_link(
        __MODULE__,
        {cluster_opts, pool_opts, alias_or_nil},
        Keyword.take(cluster_opts, [:name])
      )

    case result do
      {:ok, pid} when sync_connect_timeout == false ->
        {:ok, pid}

      {:ok, pid} ->
        ref = Process.monitor(pid)

        receive do
          :connected -> {:ok, pid}
          {:DOWN, ^ref, _, _, reason} -> {:error, reason}
        after
          sync_connect_timeout ->
            Process.demonitor(ref, [:flush])
            unalias(alias_or_nil)
            {:error, :sync_connect_timeout}
        end

      other ->
        other
    end
  end

  @spec stop(GenServer.server(), term(), timeout()) :: :ok
  def stop(pid, reason, timeout) do
    GenServer.stop(pid, reason, timeout)
  end

  @spec checkout(GenServer.server()) :: {:ok, pid()} | {:error, :empty}
  def checkout(pid) do
    GenServer.call(pid, :checkout)
  end

  ## State

  defstruct [
    # Options for the underlying connection pools.
    :pool_options,

    # When auto-discovering nodes, you cannot get their port from C*.
    # Other drivers solve this by providing a static port that the driver
    # uses to connect to any autodiscovered node.
    :autodiscovered_nodes_port,

    # A supervisor that supervises pools.
    :pool_supervisor,

    # The PID of the control connection.
    :control_connection,

    # The load balancing policy info.
    :load_balancing_module,
    :load_balancing_state,

    # The registry where connections are registered.
    :registry,

    # The number of target pools.
    :target_pools,

    # A process alias if the :sync_connect is true. This process alias is the
    # destination for the :connected message. If the :sync_connect is false,
    # this is nil.
    :sync_connect_alias,

    # The name of the cluster (if present), only used for Telemetry events.
    :name,

    # A map of peername to pool PID pairs.
    pools: %{},

    # Modules to swap processes when testing.
    xandra_mod: nil,
    control_conn_mod: nil
  ]

  ## Callbacks

  @impl true
  def init({cluster_opts, pool_opts, sync_connect_alias_or_nil}) do
    {nodes, cluster_opts} = Keyword.pop!(cluster_opts, :nodes)

    registry_name =
      Module.concat([Xandra.ClusterRegistry, to_string(System.unique_integer([:positive]))])

    {:ok, _} =
      Registry.start_link(
        keys: :unique,
        name: registry_name,
        listeners: Keyword.fetch!(cluster_opts, :registry_listeners)
      )

    # Start supervisor for the pools.
    {:ok, pool_sup} = Supervisor.start_link([], strategy: :one_for_one)

    {lb_mod, lb_opts} =
      case Keyword.fetch!(cluster_opts, :load_balancing) do
        :random -> {LoadBalancingPolicy.Random, []}
        :priority -> raise "not implemented yet"
        {mod, opts} -> {mod, opts}
      end

    state = %__MODULE__{
      pool_options: pool_opts,
      load_balancing_module: lb_mod,
      load_balancing_state: lb_mod.init(lb_opts),
      autodiscovered_nodes_port: Keyword.fetch!(cluster_opts, :autodiscovered_nodes_port),
      xandra_mod: Keyword.fetch!(cluster_opts, :xandra_module),
      control_conn_mod: Keyword.fetch!(cluster_opts, :control_connection_module),
      target_pools: Keyword.fetch!(cluster_opts, :target_pools),
      sync_connect_alias: sync_connect_alias_or_nil,
      registry: registry_name,
      name: cluster_opts[:name],
      pool_supervisor: pool_sup
    }

    {:ok, control_conn} =
      state.control_conn_mod.start_link(
        cluster: self(),
        contact_points: nodes,
        connection_options: state.pool_options,
        autodiscovered_nodes_port: state.autodiscovered_nodes_port,
        load_balancing: {lb_mod, lb_opts},
        refresh_topology_interval: Keyword.fetch!(cluster_opts, :refresh_topology_interval),
        registry: registry_name,
        name: Keyword.get(cluster_opts, :name)
      )

    state = %__MODULE__{state | control_connection: control_conn}

    # This is only for testing purposes, and is not exposed.
    if hosts = cluster_opts[:test_discovered_hosts] do
      send(self(), {:discovered_hosts, hosts})
      Enum.each(hosts, &send(self(), {:host_connected, &1}))
    end

    {:ok, state}
  end

  @impl true
  def handle_call(:checkout, _from, %__MODULE__{} = state) do
    {query_plan, state} =
      get_and_update_in(state.load_balancing_state, fn lb_state ->
        state.load_balancing_module.query_plan(lb_state)
      end)

    # Find the first host in the plan for which we have a pool.
    reply =
      query_plan
      |> Stream.map(fn %Host{} = host -> Map.fetch(state.pools, Host.to_peername(host)) end)
      |> Enum.find(_default = {:error, :empty}, &match?({:ok, _}, &1))

    {:reply, reply, state}
  end

  @impl true
  def handle_info(msg, state)

  def handle_info({:host_up, %Host{} = host}, %__MODULE__{} = state) do
    state = update_in(state.load_balancing_state, &state.load_balancing_module.host_up(&1, host))
    state = maybe_start_pools(state)
    {:noreply, state}
  end

  def handle_info({:host_connected, %Host{} = host}, %__MODULE__{} = state) do
    state =
      update_in(state.load_balancing_state, &state.load_balancing_module.host_connected(&1, host))

    state = maybe_start_pools(state)

    state =
      if alias = state.sync_connect_alias do
        send(alias, :connected)
        %__MODULE__{state | sync_connect_alias: nil}
      else
        state
      end

    {:noreply, state}
  end

  def handle_info({:host_down, %Host{} = host}, %__MODULE__{} = state) do
    state =
      update_in(state.load_balancing_state, &state.load_balancing_module.host_down(&1, host))

    state = stop_pool(state, host)
    state = maybe_start_pools(state)
    {:noreply, state}
  end

  def handle_info({:host_added, %Host{} = host}, %__MODULE__{} = state) do
    state =
      update_in(state.load_balancing_state, &state.load_balancing_module.host_added(&1, host))

    state = maybe_start_pools(state)
    {:noreply, state}
  end

  def handle_info({:host_removed, %Host{} = host}, %__MODULE__{} = state) do
    state = stop_pool(state, host)

    # Also delete the child from the supervisor altogether.
    _ = Supervisor.delete_child(state.pool_supervisor, {host.address, host.port})

    state =
      update_in(state.load_balancing_state, &state.load_balancing_module.host_removed(&1, host))

    state = update_in(state.pools, &Map.delete(&1, {host.address, host.port}))

    state = maybe_start_pools(state)
    {:noreply, state}
  end

  def handle_info({:discovered_hosts, hosts}, %__MODULE__{} = state) when is_list(hosts) do
    execute_telemetry(state, [:discovered_peers], %{peers: hosts}, _extra_meta = %{})

    state =
      Enum.reduce(hosts, state, fn %Host{} = host, acc ->
        update_in(acc.load_balancing_state, &state.load_balancing_module.host_added(&1, host))
      end)

    state = maybe_start_pools(state)
    {:noreply, state}
  end

  ## Helpers

  # This function is idempotent: you can call it as many times as you want with the same
  # peer, and it'll only start it once.
  defp start_pool(state, %Host{} = host) do
    conn_options =
      Keyword.merge(state.pool_options,
        nodes: [Host.format_address(host)],
        registry: state.registry,
        connection_listeners: [state.control_connection]
      )

    peername = Host.to_peername(host)

    pool_spec =
      Supervisor.child_spec({state.xandra_mod, conn_options},
        id: peername,
        restart: :transient
      )

    case Supervisor.start_child(state.pool_supervisor, pool_spec) do
      {:ok, pool} ->
        execute_telemetry(state, [:pool, :started], %{}, %{host: host})
        send(state.control_connection, {:started_pool, host})
        put_in(state.pools[peername], pool)

      {:error, :already_present} ->
        case Supervisor.restart_child(state.pool_supervisor, _id = peername) do
          {:ok, pool} ->
            execute_telemetry(state, [:pool, :restarted], %{}, %{host: host})
            send(state.control_connection, {:started_pool, host})
            put_in(state.pools[peername], pool)

          {:error, reason} when reason in [:running, :restarting] ->
            state

          {:error, other} ->
            raise "unexpected error when restarting pool for #{Host.format_address(host)}: #{inspect(other)}"
        end

      {:error, {:already_started, _pool}} ->
        state
    end
  end

  defp stop_pool(state, %Host{} = host) do
    _ = Supervisor.terminate_child(state.pool_supervisor, {host.address, host.port})
    update_in(state.pools, &Map.delete(&1, {host.address, host.port}))
  end

  defp maybe_start_pools(%__MODULE__{target_pools: target, pools: pools} = state)
       when map_size(pools) == target do
    state
  end

  defp maybe_start_pools(%__MODULE__{target_pools: target, pools: pools} = state)
       when map_size(pools) < target do
    {hosts_plan, state} =
      get_and_update_in(state.load_balancing_state, fn lb_state ->
        state.load_balancing_module.hosts_plan(lb_state)
      end)

    Enum.reduce_while(hosts_plan, state, fn %Host{} = host, state ->
      case Map.fetch(pools, Host.to_peername(host)) do
        {:ok, _pool} ->
          {:cont, state}

        :error ->
          state = start_pool(state, host)

          if map_size(state.pools) == target do
            {:halt, state}
          else
            {:cont, state}
          end
      end
    end)
  end

  defp execute_telemetry(%__MODULE__{} = state, event_postfix, measurements, extra_meta) do
    meta = Map.merge(%{cluster_name: state.name, cluster_pid: self()}, extra_meta)
    :telemetry.execute([:xandra, :cluster] ++ event_postfix, measurements, meta)
  end

  # TODO: remove this check once we depend on Elixir 1.15+, which requires OTP 24+,
  # where aliases were introduced.
  if function_exported?(:erlang, :alias, 1) do
    defp alias, do: :erlang.alias([:reply])
    defp unalias(alias), do: :erlang.unalias(alias)
  else
    defp alias do
      raise ArgumentError, "the :sync_connect option is only supported on Erlang/OTP 24+"
    end

    defp unalias(alias), do: raise(ArgumentError, "should never reach this")
  end
end
