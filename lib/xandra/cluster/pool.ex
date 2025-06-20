defmodule Xandra.Cluster.Pool do
  @moduledoc false

  # This module is the internal process governing the cluster itself. Xandra.Cluster
  # is just the shell that contains documentation and public API, but it's a big shell
  # and this implementation is complex, so splitting them up will make it easier
  # to work on the internals. Plus, the surface API of *this* internal module is
  # quite small, so it's really mostly internals.

  @behaviour :gen_statem

  alias Xandra.Cluster.{ConnectionPool, Host, LoadBalancingPolicy}
  alias Xandra.GenStatemHelpers

  require Record

  Record.defrecordp(:checkout_queue, [:max_size, :queue])

  ## Public API

  @spec start_link(keyword(), keyword()) :: :gen_statem.start_ret()
  def start_link(cluster_opts, connection_opts) do
    {sync_connect_timeout, cluster_opts} = Keyword.pop!(cluster_opts, :sync_connect)

    # Split out gen_statem-specific options from the cluster options.
    {gen_statem_opts, cluster_opts} = GenStatemHelpers.split_opts(cluster_opts)

    sync_connect_alias_or_nil = if sync_connect_timeout, do: Process.alias([:reply]), else: nil

    case GenStatemHelpers.start_link_with_name_registration(
           __MODULE__,
           {cluster_opts, connection_opts, sync_connect_alias_or_nil},
           gen_statem_opts
         ) do
      {:ok, pid} when is_integer(sync_connect_timeout) ->
        receive do
          {^sync_connect_alias_or_nil, :connected} ->
            {:ok, pid}
        after
          sync_connect_timeout ->
            if sync_connect_alias_or_nil, do: Process.unalias(sync_connect_alias_or_nil)
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

  @spec checkout(:gen_statem.server_ref()) ::
          {:ok, [{pid(), Host.t()}, ...]} | {:error, :empty}
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
    :connection_options,

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

    # The number of connections in each pool to a node.
    :pool_size,

    # Erlang alias to send back the ":connected" message to make :sync_connect work.
    # This is nil if :sync_connect was not used.
    :sync_connect_alias,

    # The name of the cluster (if present), only used for Telemetry events.
    :name,

    # A map of peername ({address, port}) to info about that peer.
    # Each info map is:
    # %{pool_pid: pid(), pool_ref: ref(), host: Host.t(), status: :up | :down | :connected}
    peers: %{},

    # A queue of requests that were received by this process *before* connecting
    # to *any* node. We "buffer" these for a while until we establish a connection.
    checkout_queue: %{
      queue: :queue.new(),
      max_size: nil
    },

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
  def callback_mode, do: [:handle_event_function, :state_enter]

  @impl true
  def init({cluster_opts, pool_opts, sync_connect_alias_or_nil}) do
    Process.flag(:trap_exit, true)

    # Start supervisor for the connections.
    {:ok, pool_sup} = Supervisor.start_link([], strategy: :one_for_one)

    {lb_mod, lb_opts} =
      case Keyword.fetch!(cluster_opts, :load_balancing) do
        :random -> {LoadBalancingPolicy.Random, []}
        :priority -> raise "not implemented yet"
        {mod, opts} -> {mod, opts}
      end

    checkout_queue_opts = Keyword.fetch!(cluster_opts, :queue_checkouts_before_connecting)
    checkout_queue_timeout = Keyword.fetch!(checkout_queue_opts, :timeout)
    checkout_queue_max_size = Keyword.fetch!(checkout_queue_opts, :max_size)

    data = %__MODULE__{
      connection_options: pool_opts,
      contact_nodes: Keyword.fetch!(cluster_opts, :nodes),
      load_balancing_module: lb_mod,
      load_balancing_state: lb_mod.init(lb_opts),
      autodiscovered_nodes_port: Keyword.fetch!(cluster_opts, :autodiscovered_nodes_port),
      xandra_mod: Keyword.fetch!(cluster_opts, :xandra_module),
      control_conn_mod: Keyword.fetch!(cluster_opts, :control_connection_module),
      target_pools: Keyword.fetch!(cluster_opts, :target_pools),
      name: Keyword.get(cluster_opts, :name),
      pool_size: Keyword.fetch!(cluster_opts, :pool_size),
      pool_supervisor: pool_sup,
      refresh_topology_interval: Keyword.fetch!(cluster_opts, :refresh_topology_interval),
      checkout_queue: checkout_queue(queue: :queue.new(), max_size: checkout_queue_max_size),
      sync_connect_alias: sync_connect_alias_or_nil
    }

    actions = [
      {:next_event, :internal, :start_control_connection},
      timeout_action(:flush_checkout_queue, checkout_queue_timeout)
    ]

    {:ok, :never_connected, data, actions}
  end

  @impl true
  def handle_event(type, event, state, data)

  def handle_event(:enter, :never_connected, :never_connected, _data) do
    :keep_state_and_data
  end

  # This is the only state transition we can do: :never_connected -> :has_connected_once.
  # We can never go the other way, of course.
  def handle_event(:enter, _from = :never_connected, _to = :has_connected_once, data) do
    {data, actions} = flush_checkout_queue(data)
    {:keep_state, data, actions}
  end

  def handle_event(:internal, :start_control_connection, _state, data) do
    case start_control_connection(data) do
      {:ok, data} -> {:keep_state, data}
      :error -> {:keep_state_and_data, timeout_action(:reconnect_control_connection, 1000)}
    end
  end

  def handle_event({:timeout, :flush_checkout_queue}, nil, :never_connected, %__MODULE__{} = data) do
    {checkout_queue(queue: queue), data} = get_and_update_in(data.checkout_queue, &{&1, nil})
    reply_actions = for from <- :queue.to_list(queue), do: {:reply, from, {:error, :empty}}
    {:keep_state, data, reply_actions}
  end

  # We have never connected, but we already flushed once, so we won't keep adding requests to
  # the queue.
  def handle_event({:call, from}, :checkout, :never_connected, %__MODULE__{checkout_queue: nil}) do
    {:keep_state_and_data, {:reply, from, {:error, :empty}}}
  end

  def handle_event({:call, from}, :checkout, :never_connected, %__MODULE__{} = data) do
    checkout_queue(queue: queue, max_size: max_size) = data.checkout_queue

    if :queue.len(queue) == max_size do
      {:keep_state_and_data, {:reply, from, {:error, :empty}}}
    else
      data =
        put_in(
          data.checkout_queue,
          checkout_queue(data.checkout_queue, queue: :queue.in(from, queue))
        )

      {:keep_state, data}
    end
  end

  def handle_event({:call, from}, :checkout, :has_connected_once, %__MODULE__{} = data) do
    {data, reply_action} = checkout_connection(data, from)
    {:keep_state, data, reply_action}
  end

  def handle_event({:call, from}, :connected_hosts, _state, %__MODULE__{} = data) do
    connected_hosts =
      for %{status: :connected, pool_pid: pool_pid, host: host} <- Map.values(data.peers),
          is_pid(pool_pid) do
        host
      end

    {:keep_state_and_data, {:reply, from, connected_hosts}}
  end

  def handle_event(:info, {:host_up, address, port}, _state, %__MODULE__{} = data) do
    # Set the host's status as :up if its state had been previously marked as :down.
    data =
      case data.peers[{address, port}] do
        %{status: :down} ->
          set_host_status(data, {address, port}, :up)

        # github.com/whatyouhide/xandra/issues/371 we want to refresh topology for hosts we don't know
        nil ->
          send(data.control_connection, :refresh_topology)
          data

        _ ->
          data
      end

    data = maybe_start_pools(data)
    {:keep_state, data}
  end

  def handle_event(:info, {:host_down, address, port}, _state, %__MODULE__{} = data) do
    # Set the host's status as :down, regardless of its current state.
    peername = {address, port}
    data = set_host_status(data, peername, :down)
    host = data.peers[peername].host
    data = stop_pool(data, host)
    data = maybe_start_pools(data)
    {:keep_state, data}
  end

  def handle_event(:info, {:discovered_hosts, new_peers}, _state, %__MODULE__{} = data)
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

    data = maybe_start_pools(data)
    {:keep_state, data}
  end

  # Sent by the connection itself.
  # Whatever the state is, we move to the :has_connected_once state. If the current state
  # is :never_connected, that means that this will result in a state transition, which
  # triggers other events and stuff.
  def handle_event(
        :info,
        {:xandra, :connected, peername, _pid},
        _state,
        %__MODULE__{} = data
      )
      when is_peername(peername) do
    data = set_host_status(data, peername, :connected)

    if alias = data.sync_connect_alias do
      send(alias, {alias, :connected})
    end

    {:next_state, :has_connected_once, data}
  end

  # Sent by the connection itself.
  def handle_event(
        :info,
        {:xandra, :disconnected, peername, _pid},
        _state,
        %__MODULE__{} = data
      )
      when is_peername(peername) do
    # Not connected anymore, but we're not really sure if the whole host is down.
    data = set_host_status(data, peername, :up)
    data = stop_pool(data, data.peers[peername].host)
    # There might be more hosts that we could connect to instead of the stopped one
    data = maybe_start_pools(data)
    {:keep_state, data}
  end

  # Sent by the connection itself.
  def handle_event(
        :info,
        {:xandra, :failed_to_connect, peername, _pid},
        _state,
        %__MODULE__{} = data
      ) do
    if data.peers[peername] do
      data = set_host_status(data, peername, :down)
      host = data.peers[peername].host
      data = stop_pool(data, host)
      # There might be more hosts that we could connect to instead of the stopped one
      data = maybe_start_pools(data)
      {:keep_state, data}
    else
      {:keep_state, data}
    end
  end

  # Handle the control connection shutting itself down.
  # If we don't have a control connection PID (yet?) and some other PID shuts down
  # with reason :shutdown, we assume it's the control connection we go on to
  # starting the next control connection.
  def handle_event(
        :info,
        {:EXIT, control_connection_pid, {:shutdown, _reason}},
        _state,
        %__MODULE__{control_connection: data_pid}
      )
      when data_pid == control_connection_pid or is_nil(data_pid) do
    {:keep_state_and_data, {:next_event, :internal, :start_control_connection}}
  end

  # Propagate all unhandled exits by exiting with the same reason. After all, if the control
  # connection process or the pool supervisor *crash*, we want to crash this so that
  # the whole thing is restarted.
  def handle_event(:info, {:EXIT, _pid, reason}, _state, %__MODULE__{} = _data) do
    exit(reason)
  end

  def handle_event(:info, {:DOWN, ref, _, _, _reason}, _state, %__MODULE__{} = data) do
    # Find the pool that went down, so that we can clean it up.
    {peername, _info} = Enum.find(data.peers, fn {_peername, info} -> info.pool_ref == ref end)
    data = put_in(data.peers[peername].pool_pid, nil)
    data = put_in(data.peers[peername].pool_ref, nil)
    data = set_host_status(data, peername, :up)
    data = maybe_start_pools(data)
    {:keep_state, data}
  end

  def handle_event({:timeout, :reconnect_control_connection}, nil, _state, %__MODULE__{} = data) do
    {:keep_state, data, {:next_event, :internal, :start_control_connection}}
  end

  @impl true
  def terminate(reason, _state, %__MODULE__{} = data) do
    try do
      Supervisor.stop(data.pool_supervisor)
    catch
      :exit, {:noproc, _} -> :ok
    end

    try do
      data.control_conn_mod.stop(data.control_connection)
    catch
      :exit, {:noproc, _} -> :ok
    end

    reason
  end

  ## Helpers

  defp checkout_connection(data, from) do
    {query_plan, data} =
      get_and_update_in(data.load_balancing_state, fn lb_state ->
        data.load_balancing_module.query_plan(lb_state)
      end)

    # Find all connected hosts
    connected_hosts =
      for host <- query_plan,
          %{pool_pid: pool_pid, host: host} = Map.get(data.peers, Host.to_peername(host)),
          not is_nil(host),
          is_pid(pool_pid) and Process.alive?(pool_pid),
          pid = ConnectionPool.checkout(pool_pid),
          do: {pid, host}

    reply =
      case connected_hosts do
        [] -> {:error, :empty}
        connected_hosts -> {:ok, connected_hosts}
      end

    {data, {:reply, from, reply}}
  end

  defp handle_host_added(%__MODULE__{} = data, %Host{} = host) do
    data =
      update_in(data.load_balancing_state, &data.load_balancing_module.host_added(&1, host))

    data =
      put_in(data.peers[Host.to_peername(host)], %{
        host: host,
        status: :up,
        pool_pid: nil,
        pool_ref: nil
      })

    execute_telemetry(data, [:change_event], %{}, %{event_type: :host_added, host: host})

    data
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
      Keyword.merge(data.connection_options,
        nodes: [Host.format_address(host)],
        cluster_pid: self()
      )

    peername = Host.to_peername(host)

    pool_spec =
      Supervisor.child_spec(
        {data.xandra_mod, connection_options: conn_options, pool_size: data.pool_size},
        id: peername,
        restart: :transient
      )

    case Supervisor.start_child(data.pool_supervisor, pool_spec) do
      {:ok, pool} ->
        execute_telemetry(data, [:pool, :started], %{}, %{host: host})
        pool_ref = Process.monitor(pool)
        data = put_in(data.peers[peername].pool_pid, pool)
        put_in(data.peers[peername].pool_ref, pool_ref)

      {:error, :already_present} ->
        case Supervisor.restart_child(data.pool_supervisor, _id = peername) do
          {:ok, pool} ->
            execute_telemetry(data, [:pool, :restarted], %{}, %{host: host})
            pool_ref = Process.monitor(pool)
            data = put_in(data.peers[peername].pool_pid, pool)
            put_in(data.peers[peername].pool_ref, pool_ref)

          {:error, reason} when reason in [:running, :restarting] ->
            data

          {:error, other} ->
            raise "unexpected error when restarting pool for #{Host.format_address(host)}: #{inspect(other)}"
        end

      {:error, {:already_started, _pool}} ->
        data
    end
  end

  # We refresh the topology if we get an unknown host, see: https://github.com/whatyouhide/xandra/issues/384
  defp set_host_status(
         %__MODULE__{peers: peers, control_connection: conn} = data,
         peername,
         _new_status
       )
       when not is_map_key(peers, peername) do
    send(conn, :refresh_topology)
    data
  end

  defp set_host_status(data, peername, new_status) when new_status in [:up, :down, :connected] do
    {%Host{} = host, data} =
      get_and_update_in(data.peers[peername], fn %{host: host} = peer ->
        {host, %{peer | status: new_status}}
      end)

    lb_callback =
      case new_status do
        :up -> :host_up
        :down -> :host_down
        :connected -> :host_connected
      end

    update_in(
      data.load_balancing_state,
      &apply(data.load_balancing_module, lb_callback, [&1, host])
    )
  end

  defp stop_pool(data, %Host{} = host) do
    peername = Host.to_peername(host)

    if monitor_ref = data.peers[peername].pool_ref do
      Process.demonitor(monitor_ref, [:flush])
    end

    execute_telemetry(data, [:pool, :stopped], %{}, %{host: host})
    _ = Supervisor.terminate_child(data.pool_supervisor, peername)

    data = put_in(data.peers[peername].pool_pid, nil)
    data = put_in(data.peers[peername].pool_ref, nil)

    data
  end

  defp maybe_start_pools(%__MODULE__{target_pools: target} = data) do
    if Enum.count(data.peers, fn {_peername, %{pool_pid: pid}} -> is_pid(pid) end) == target do
      data
    else
      {hosts_plan, data} =
        get_and_update_in(data.load_balancing_state, &data.load_balancing_module.hosts_plan/1)

      Enum.reduce_while(hosts_plan, data, fn %Host{} = host, data_acc ->
        case Map.fetch!(data_acc.peers, Host.to_peername(host)) do
          %{pool_pid: pid} when is_pid(pid) ->
            {:cont, data_acc}

          %{pool_pid: nil} ->
            data_acc = start_pool(data_acc, host)

            if Enum.count(data_acc.peers, fn {_peername, %{pool_pid: pid}} -> is_pid(pid) end) ==
                 target do
              {:halt, data_acc}
            else
              {:cont, data_acc}
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
      connection_options: data.connection_options,
      autodiscovered_nodes_port: data.autodiscovered_nodes_port,
      refresh_topology_interval: data.refresh_topology_interval
    ]

    case data.control_conn_mod.start_link(control_conn_opts) do
      {:ok, control_conn} ->
        {:ok, control_conn}

      {:error, {:__caught__, kind, reason, stacktrace}} ->
        :erlang.raise(kind, reason, stacktrace)

      {:error, _reason} ->
        start_control_connection(data, hosts)
    end
  end

  defp flush_checkout_queue(%__MODULE__{} = data) do
    {checkout_queue(queue: queue), data} = get_and_update_in(data.checkout_queue, &{&1, nil})

    {reply_actions, data} =
      Enum.map_reduce(:queue.to_list(queue), data, fn from, data ->
        {data, reply_action} = checkout_connection(data, from)
        {reply_action, data}
      end)

    cancel_timeout_action = timeout_action(:flush_checkout_queue, :infinity)
    {data, [cancel_timeout_action] ++ reply_actions}
  end

  defp timeout_action(name, time) do
    {{:timeout, name}, time, _event_content = nil}
  end

  defp execute_telemetry(%__MODULE__{} = state, event_postfix, measurements, extra_meta) do
    meta = Map.merge(%{cluster_name: state.name, cluster_pid: self()}, extra_meta)
    :telemetry.execute([:xandra, :cluster] ++ event_postfix, measurements, meta)
  end
end
