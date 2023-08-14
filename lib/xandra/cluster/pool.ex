defmodule Xandra.Cluster.Pool do
  @moduledoc false

  # This module is the internal process governing the cluster itself. Xandra.Cluster
  # is just the shell that contains documentation and public API, but it's a big shell
  # and this implementation is complex, so splitting them up will make it easier
  # to work on the internals. Plus, the surface API of *this* internal module is
  # quite small, so it's really mostly internals.

  @behaviour :gen_statem

  alias Xandra.Cluster.{Host, LoadBalancingPolicy}

  @genstatem_opts [:debug, :hibernate_after, :spawn_opt]
  @state :no_state

  ## Public API

  @spec start_link(keyword(), keyword()) :: GenServer.on_start()
  def start_link(cluster_opts, pool_opts) do
    {sync_connect_timeout, cluster_opts} = Keyword.pop!(cluster_opts, :sync_connect)

    # Split out GenServer-specific options from the cluster options.
    {genstatem_opts, cluster_opts} = Keyword.split(cluster_opts, @genstatem_opts)
    genstatem_opts = Keyword.merge(genstatem_opts, Keyword.take(cluster_opts, [:name]))

    alias_or_nil = if sync_connect_timeout, do: alias()

    start_arg = {cluster_opts, pool_opts, alias_or_nil}

    result =
      case Keyword.fetch(cluster_opts, :name) do
        :error ->
          :gen_statem.start_link(__MODULE__, start_arg, genstatem_opts)

        {:ok, atom} when is_atom(atom) ->
          :gen_statem.start_link({:local, atom}, __MODULE__, start_arg, genstatem_opts)

        {:ok, {:global, _term} = tuple} ->
          :gen_statem.start_link(tuple, __MODULE__, start_arg, genstatem_opts)

        {:ok, {:via, via_module, _term} = tuple} when is_atom(via_module) ->
          :gen_statem.start_link(tuple, __MODULE__, start_arg, genstatem_opts)

        {:ok, other} ->
          raise ArgumentError, """
          expected :name option to be one of the following:

            * nil
            * atom
            * {:global, term}
            * {:via, module, term}

          Got: #{inspect(other)}
          """
      end

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

  @spec stop(:gen_statem.server_ref(), term(), timeout()) :: :ok
  def stop(pid, reason, timeout) do
    :gen_statem.stop(pid, reason, timeout)
  end

  @spec checkout(:gen_statem.server_ref()) :: {:ok, pid()} | {:error, :empty}
  def checkout(pid) do
    :gen_statem.call(pid, :checkout)
  end

  @spec connected_hosts(:gen_statem.server_ref()) :: [Host.t()]
  def connected_hosts(pid) do
    :gen_statem.call(pid, :connected_hosts)
  end

  ## Data

  defstruct [
    # Options for the underlying connection pools.
    :pool_options,

    # Contact nodes.
    :contact_nodes,

    # When auto-discovering nodes, you cannot get their port from C*.
    # Other drivers solve this by providing a static port that the driver
    # uses to connect to any autodiscovered node.
    :autodiscovered_nodes_port,

    # A supervisor that supervises pools.
    :pool_supervisor,

    # The PID of the control connection.
    :control_connection,

    # The interval to refresh the cluster's topology. Usually passed down
    # to the control connection.
    :refresh_topology_interval,

    # The load balancing policy info.
    :load_balancing_module,
    :load_balancing_state,

    # The number of target pools.
    :target_pools,

    # A process alias if the :sync_connect is true. This process alias is the
    # destination for the :connected message. If the :sync_connect is false,
    # this is nil.
    :sync_connect_alias,

    # The name of the cluster (if present), only used for Telemetry events.
    :name,

    # A map of peername ({address, port}) to info about that peer.
    # Each info map is:
    # %{pool_pid: pid(), host: Host.t(), status: :up | :down | :connected}
    peers: %{},

    # Modules to swap processes when testing.
    xandra_mod: nil,
    control_conn_mod: nil
  ]

  ## Guards

  defguardp is_peername(term)
            when is_tuple(term) and tuple_size(term) == 2 and is_tuple(elem(term, 0)) and
                   is_integer(elem(term, 1))

  ## Callbacks

  @impl true
  def callback_mode, do: :handle_event_function

  @impl true
  def init({cluster_opts, pool_opts, sync_connect_alias_or_nil}) do
    Process.flag(:trap_exit, true)

    # Start supervisor for the pools.
    {:ok, pool_sup} = Supervisor.start_link([], strategy: :one_for_one)

    {lb_mod, lb_opts} =
      case Keyword.fetch!(cluster_opts, :load_balancing) do
        :random -> {LoadBalancingPolicy.Random, []}
        :priority -> raise "not implemented yet"
        {mod, opts} -> {mod, opts}
      end

    data =
      %__MODULE__{
        pool_options: pool_opts,
        contact_nodes: Keyword.fetch!(cluster_opts, :nodes),
        load_balancing_module: lb_mod,
        load_balancing_state: lb_mod.init(lb_opts),
        autodiscovered_nodes_port: Keyword.fetch!(cluster_opts, :autodiscovered_nodes_port),
        xandra_mod: Keyword.fetch!(cluster_opts, :xandra_module),
        control_conn_mod: Keyword.fetch!(cluster_opts, :control_connection_module),
        target_pools: Keyword.fetch!(cluster_opts, :target_pools),
        sync_connect_alias: sync_connect_alias_or_nil,
        name: Keyword.get(cluster_opts, :name),
        pool_supervisor: pool_sup,
        refresh_topology_interval: Keyword.fetch!(cluster_opts, :refresh_topology_interval)
      }

    {:ok, @state, data, {:next_event, :internal, :start_control_connection}}
  end

  @impl true
  def handle_event(type, event, state, data)

  def handle_event(:internal, :start_control_connection, @state, data) do
    case start_control_connection(data) do
      {:ok, data} ->
        {:keep_state, data}

      :error ->
        {:keep_state, data, {{:timeout, :reconnect_control_connection}, 1000, nil}}
    end
  end

  def handle_event({:call, from}, :checkout, @state, %__MODULE__{} = data) do
    {query_plan, data} =
      get_and_update_in(data.load_balancing_state, fn lb_state ->
        data.load_balancing_module.query_plan(lb_state)
      end)

    # Find the first host in the plan for which we have a pool.
    reply =
      query_plan
      |> Stream.map(fn %Host{} = host -> Map.fetch(data.peers, Host.to_peername(host)) end)
      |> Enum.find_value(_default = {:error, :empty}, fn
        {:ok, %{pool_pid: pid}} when is_pid(pid) -> {:ok, pid}
        _other -> nil
      end)

    {:keep_state, data, {:reply, from, reply}}
  end

  def handle_event({:call, from}, :connected_hosts, @state, %__MODULE__{} = data) do
    connected_hosts =
      for %{status: :connected, pool_pid: pool_pid, host: host} <- Map.values(data.peers),
          is_pid(pool_pid) do
        host
      end

    {:keep_state_and_data, {:reply, from, connected_hosts}}
  end

  def handle_event(:into, {:host_up, %Host{} = host}, @state, %__MODULE__{} = data) do
    data = update_in(data.load_balancing_state, &data.load_balancing_module.host_up(&1, host))

    data =
      update_in(data.peers[Host.to_peername(host)], fn
        %{status: :down} = peer -> %{peer | status: :up}
        peer -> peer
      end)

    data = maybe_start_pools(data)
    {:keep_state, data}
  end

  def handle_event(:info, {:host_down, %Host{} = host}, @state, %__MODULE__{} = data) do
    data =
      update_in(data.load_balancing_state, &data.load_balancing_module.host_down(&1, host))

    data = put_in(data.peers[Host.to_peername(host)].status, :down)

    data = stop_pool(data, host)
    data = maybe_start_pools(data)
    {:keep_state, data}
  end

  def handle_event(:info, {:discovered_hosts, new_peers}, @state, %__MODULE__{} = data)
      when is_list(new_peers) do
    execute_telemetry(data, [:discovered_peers], %{peers: new_peers}, _extra_meta = %{})

    new_peers_map = Map.new(new_peers, &{Host.to_peername(&1), &1})
    new_peers_set = MapSet.new(new_peers, &Host.to_peername/1)
    old_peers_set = data.peers |> Map.keys() |> MapSet.new()

    # Find the peers that are not in the set of known peers anymore, remove them
    # from the LBP, emit a telemetry event, stop the pools.
    data =
      Enum.reduce(MapSet.difference(old_peers_set, new_peers_set), data, fn peername, data_acc ->
        %{host: %Host{} = host} = Map.fetch!(data_acc.peers, peername)
        handle_host_removed(data_acc, host)
      end)

    # For the new peers that we didn't know about, add them to the LBP, emit a
    # telemetry event, and potentially start pools.
    data =
      Enum.reduce(MapSet.difference(new_peers_set, old_peers_set), data, fn peername, data_acc ->
        handle_host_added(data_acc, Map.fetch!(new_peers_map, peername))
      end)

    {:keep_state, data}
  end

  # Sent by the connection itself.
  def handle_event(
        :info,
        {:xandra, :connected, peername, _pid},
        @state,
        %__MODULE__{} = data
      )
      when is_peername(peername) do
    data = put_in(data.peers[peername].status, :connected)

    host = data.peers[peername].host

    data =
      update_in(data.load_balancing_state, &data.load_balancing_module.host_connected(&1, host))

    data =
      if alias = data.sync_connect_alias do
        send(alias, :connected)
        %__MODULE__{data | sync_connect_alias: nil}
      else
        data
      end

    {:keep_state, data}
  end

  # Sent by the connection itself.
  def handle_event(
        :info,
        {:xandra, :failed_to_connect, peername, _pid},
        @state,
        %__MODULE__{} = data
      ) do
    data = put_in(data.peers[peername].status, :down)
    data = stop_pool(data, data.peers[peername].host)
    {:keep_state, data}
  end

  def handle_event(:info, {:EXIT, _pid, reason}, @state, %__MODULE__{} = _data)
      when reason in [:normal, :shutdown] or
             (is_tuple(reason) and tuple_size(reason) == 2 and elem(reason, 0) == :shutdown) do
    :keep_state_and_data
  end

  def handle_event({:timeout, :reconnect_control_connection}, nil, @state, %__MODULE__{} = data) do
    {:keep_state, data, {:next_event, :internal, :start_control_connection}}
  end

  ## Helpers

  defp handle_host_added(%__MODULE__{} = data, %Host{} = host) do
    data =
      update_in(data.load_balancing_state, &data.load_balancing_module.host_added(&1, host))

    data =
      update_in(
        data.peers,
        &Map.put(&1, Host.to_peername(host), %{host: host, status: :up, pool_pid: nil})
      )

    execute_telemetry(data, [:change_event], %{}, %{event_type: :host_added, host: host})

    maybe_start_pools(data)
  end

  defp handle_host_removed(%__MODULE__{} = data, %Host{} = host) do
    data =
      update_in(data.load_balancing_state, &data.load_balancing_module.host_removed(&1, host))

    data = stop_pool(data, host)
    _ = Supervisor.delete_child(data.pool_supervisor, Host.to_peername(host))

    data = update_in(data.peers, &Map.delete(&1, Host.to_peername(host)))

    execute_telemetry(data, [:change_event], %{}, %{event_type: :host_removed, host: host})

    data
  end

  # This function is idempotent: you can call it as many times as you want with the same
  # peer, and it'll only start it once.
  defp start_pool(%__MODULE__{} = data, %Host{} = host) do
    conn_options =
      Keyword.merge(data.pool_options, nodes: [Host.format_address(host)], cluster_pid: self())

    peername = Host.to_peername(host)

    pool_spec =
      Supervisor.child_spec({data.xandra_mod, conn_options}, id: peername, restart: :transient)

    case Supervisor.start_child(data.pool_supervisor, pool_spec) do
      {:ok, pool} ->
        execute_telemetry(data, [:pool, :started], %{}, %{host: host})
        put_in(data.peers[peername].pool_pid, pool)

      {:error, :already_present} ->
        case Supervisor.restart_child(data.pool_supervisor, _id = peername) do
          {:ok, pool} ->
            execute_telemetry(data, [:pool, :restarted], %{}, %{host: host})
            put_in(data.peers[peername].pool_pid, pool)

          {:error, reason} when reason in [:running, :restarting] ->
            data

          {:error, other} ->
            raise "unexpected error when restarting pool for #{Host.format_address(host)}: #{inspect(other)}"
        end

      {:error, {:already_started, _pool}} ->
        data
    end
  end

  defp stop_pool(data, %Host{} = host) do
    peername = Host.to_peername(host)

    execute_telemetry(data, [:pool, :stopped], %{}, %{host: host})
    _ = Supervisor.terminate_child(data.pool_supervisor, peername)

    put_in(data.peers[peername].pool_pid, nil)
  end

  defp maybe_start_pools(%__MODULE__{target_pools: target} = data) do
    if Enum.count(data.peers, fn {_peername, %{pool_pid: pid}} -> is_pid(pid) end) == target do
      data
    else
      {hosts_plan, data} =
        get_and_update_in(data.load_balancing_state, &data.load_balancing_module.hosts_plan/1)

      Enum.reduce_while(hosts_plan, data, fn %Host{} = host, state ->
        case Map.fetch!(data.peers, Host.to_peername(host)) do
          %{pool_pid: pid} when is_pid(pid) ->
            {:cont, state}

          %{pool_pid: nil} ->
            data = start_pool(data, host)

            if Enum.count(data.peers, fn {_peername, %{pool_pid: pid}} -> is_pid(pid) end) ==
                 target do
              {:halt, data}
            else
              {:cont, data}
            end
        end
      end)
    end
  end

  defp start_control_connection(%__MODULE__{} = data) do
    {lbp_hosts, data} =
      get_and_update_in(data.load_balancing_state, fn lb_state ->
        data.load_balancing_module.query_plan(lb_state)
      end)

    contact_nodes_as_hosts =
      Enum.map(data.contact_nodes, fn {address, port} ->
        %Host{address: address, port: port}
      end)

    case start_control_connection(data, lbp_hosts ++ contact_nodes_as_hosts) do
      {:ok, control_conn} -> {:ok, %__MODULE__{data | control_connection: control_conn}}
      :error -> :error
    end
  end

  defp start_control_connection(_data, []) do
    :error
  end

  defp start_control_connection(data, [%Host{} = host | hosts]) do
    control_conn_opts = [
      cluster_pid: self(),
      cluster_name: data.name,
      contact_node: {host.address, host.port},
      connection_options: data.pool_options,
      autodiscovered_nodes_port: data.autodiscovered_nodes_port,
      refresh_topology_interval: data.refresh_topology_interval
    ]

    case data.control_conn_mod.start_link(control_conn_opts) do
      {:ok, control_conn} -> {:ok, control_conn}
      {:error, _reason} -> start_control_connection(data, hosts)
    end
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
