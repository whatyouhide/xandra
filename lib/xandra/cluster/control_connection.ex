defmodule Xandra.Cluster.ControlConnection do
  @moduledoc false

  @behaviour :gen_statem

  alias Xandra.{Frame, Simple, Connection.Utils}
  alias Xandra.Cluster.{Host, LoadBalancingPolicy, StatusChange, TopologyChange}

  require Logger

  @default_backoff 5_000
  @default_timeout 5_000
  @healthcheck_timeout 500
  @forced_transport_options [packet: :raw, mode: :binary, active: false]
  @delay_after_topology_change 5_000

  # Internal NimbleOptions schema used to validate the options given to start_link/1.
  # This is only used for internal consistency and having an additional layer of
  # weak "type checking" (some people might get angry at this).
  @opts_schema [
    cluster: [type: :pid, required: true],
    contact_points: [type: :any, required: true],
    connection_options: [type: :keyword_list, required: true],
    autodiscovered_nodes_port: [type: :non_neg_integer, required: true],
    load_balancing: [type: :mod_arg, required: true],
    refresh_topology_interval: [type: :timeout, required: true],
    registry: [type: :atom, required: true],
    name: [type: :any]
  ]

  defstruct [
    # The PID of the parent cluster.
    :cluster,

    # A list of {address, port} contact points.
    :contact_points,

    # The transport and its options, used to connect to the provided nodes.
    :transport,
    :transport_options,

    # The options to use to connect to the nodes.
    :options,

    # The same as in the cluster.
    :autodiscovered_nodes_port,

    # The interval at which to refresh the cluster topology.
    :refresh_topology_interval,

    # The load balancing policy, as a {mod, state} tuple, and the options.
    :lbp,
    :lb_opts,

    # The registry to use to register connections.
    :registry,

    # The name of the cluster (as given through the :name option).
    :name,

    # A map of {ip, port} => %{host: %Host{}, status: atom}.
    peers: %{},

    # Data buffer.
    buffer: <<>>
  ]

  defmodule ConnectedNode do
    @moduledoc false
    defstruct [:socket, :protocol_module, :ip, :port, :host]
  end

  # Need to manually define child_spec/1 because :gen_statem doesn't provide any utilities
  # around that.
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(options) when is_list(options) do
    %{id: __MODULE__, type: :worker, start: {__MODULE__, :start_link, [options]}}
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) when is_list(options) do
    options = NimbleOptions.validate!(options, @opts_schema)

    connection_options = Keyword.fetch!(options, :connection_options)

    transport = if connection_options[:encryption], do: :ssl, else: :gen_tcp

    {transport_options, connection_options} =
      Keyword.pop(connection_options, :transport_options, [])

    contact_points =
      options
      |> Keyword.fetch!(:contact_points)
      |> contact_points_to_hosts()

    {lb_module, lb_opts} = Keyword.fetch!(options, :load_balancing)

    data = %__MODULE__{
      cluster: Keyword.fetch!(options, :cluster),
      contact_points: contact_points,
      autodiscovered_nodes_port: Keyword.fetch!(options, :autodiscovered_nodes_port),
      refresh_topology_interval: Keyword.fetch!(options, :refresh_topology_interval),
      lbp: {lb_module, lb_module.init(lb_opts)},
      lb_opts: lb_opts,
      options: connection_options,
      transport: transport,
      transport_options: Keyword.merge(transport_options, @forced_transport_options),
      registry: Keyword.fetch!(options, :registry),
      name: Keyword.get(options, :name)
    }

    :gen_statem.start_link(__MODULE__, data, [])
  end

  ## Callbacks

  # The possible states are:
  #
  # * :disconnected - the control connection is not connected to any node
  # * {:connected, %ConnectedNode{}} - the control connection is connected to the given node

  @impl :gen_statem
  def init(data) do
    {:ok, :disconnected, data, {:next_event, :internal, :connect}}
  end

  @impl :gen_statem
  def callback_mode, do: [:handle_event_function, :state_enter]

  # Disconnected state

  @impl true
  def handle_event(type, content, state, data)

  def handle_event(:enter, state, state, _data) do
    :keep_state_and_data
  end

  # If we connect successfully, we set up a timer to periodically refresh the topology.
  def handle_event(:enter, _old = :disconnected, _new = {:connected, node}, data) do
    execute_telemetry(data, [:control_connection, :connected], %{}, %{host: node.host})
    {:keep_state_and_data, {{:timeout, :refresh_topology}, data.refresh_topology_interval, nil}}
  end

  # If we disconnect, we cancel the timer for the periodic refresh.
  def handle_event(:enter, _old = {:connected, _node}, _new = :disconnected, _data) do
    timeouts_to_cancel =
      for name <- [:refresh_topology, :delayed_topology_change] do
        {{:timeout, name}, :infinity, nil}
      end

    {:keep_state_and_data, timeouts_to_cancel}
  end

  # Connecting is the hardest thing control connections do. The gist is this:
  #
  #   1. We try to connect to each node in :seed_peernames until one succeeds
  #   2. We discover the peers for that node
  #   3. We register to the events for that node
  #   4. We send the discovered peers back to the cluster alongside the connected node
  #   5. We move to the state {:connected, node}
  def handle_event(:internal, :connect, :disconnected, %__MODULE__{} = data) do
    case connect_to_first_available_node(data) do
      {:ok, connected_node, peers} ->
        data = refresh_topology(data, peers)
        {:next_state, {:connected, connected_node}, data}

      :error ->
        {:keep_state_and_data, {{:timeout, :reconnect}, @default_backoff, nil}}
    end
  end

  # TCP/SSL messages that we get when we're already in the "disconnected" state can
  # be safely ignored.
  def handle_event(:info, msg, :disconnected, %__MODULE__{})
      when is_tuple(msg) and elem(msg, 0) in [:tcp_error, :ssl_error, :tcp_closed, :ssl_closed] do
    :keep_state_and_data
  end

  # Postpone healthcheck for after control connection is established
  def handle_event(:info, {:healthcheck, _host}, :disconnected, _data) do
    {:keep_state_and_data, :postpone}
  end

  def handle_event({:timeout, {:check_host_health, _host}}, nil, :disconnected, _data) do
    {:keep_state_and_data, :postpone}
  end

  # Postpone these messages for when the connection is connected.
  def handle_event(:info, {event, pid}, :disconnected, %__MODULE__{} = _data)
      when event in [:connected, :disconnected] and is_pid(pid) do
    {:keep_state_and_data, :postpone}
  end

  # Trigger the reconnect event once the timer expires.
  def handle_event({:timeout, :reconnect}, _content, :disconnected, _data) do
    {:keep_state_and_data, {:next_event, :internal, :connect}}
  end

  # Connected state

  def handle_event(
        {:timeout, :refresh_topology},
        nil,
        {:connected, %ConnectedNode{socket: socket} = node},
        %__MODULE__{} = data
      ) do
    with :ok <- inet_mod(data.transport).setopts(socket, active: false),
         :ok <- assert_no_transport_message(socket),
         {:ok, peers} <- fetch_cluster_topology(data, node),
         :ok <- inet_mod(data.transport).setopts(socket, active: :once) do
      data = refresh_topology(data, peers)
      {:keep_state, data, {{:timeout, :refresh_topology}, data.refresh_topology_interval, nil}}
    else
      {:error, reason} ->
        _ = data.transport.close(socket)
        execute_disconnected_telemetry(data, node, reason)
        {:next_state, :disconnected, data, {:next_event, :internal, :connect}}
    end
  end

  def handle_event(
        {:timeout, :delayed_topology_change},
        %TopologyChange{effect: "NEW_NODE", address: address},
        {:connected, connected_node},
        %__MODULE__{} = data
      ) do
    select_peers_query =
      "SELECT peer, data_center, host_id, rack, release_version, schema_version, tokens FROM system.peers"

    with {:ok, peers} <- query(data, connected_node, select_peers_query),
         host when not is_nil(host) <- Enum.find(peers, fn peer -> peer["peer"] == address end) do
      new_host = queried_peer_to_host(host)
      new_host = %Host{new_host | port: data.autodiscovered_nodes_port}

      execute_telemetry(data, [:change_event], %{}, %{
        event_type: :host_added,
        host: new_host,
        changed: true,
        source: :cassandra
      })

      data = update_in(data.lbp, &LoadBalancingPolicy.update_host(&1, host, :added))
      send(data.cluster, {:host_added, new_host})
      put_in(data.peers[{new_host.address, new_host.port}], %{status: :up, host: host, last_seen_at:  System.system_time(:millisecond)})
    else
      _ -> data
    end
  end

  def handle_event(
        :info,
        {kind, socket, reason},
        {:connected, %ConnectedNode{socket: socket} = node},
        %__MODULE__{} = data
      )
      when kind in [:tcp_error, :ssl_error] do
    _ = data.transport.close(socket)
    execute_disconnected_telemetry(data, node, reason)
    {:next_state, :disconnected, data, {:next_event, :internal, :connect}}
  end

  def handle_event(
        :info,
        {kind, socket},
        {:connected, %ConnectedNode{socket: socket} = node},
        %__MODULE__{} = data
      )
      when kind in [:tcp_closed, :ssl_closed] do
    _ = data.transport.close(socket)
    execute_disconnected_telemetry(data, node, :closed)
    data = %__MODULE__{data | buffer: <<>>}
    {:next_state, :disconnected, data, {:next_event, :internal, :connect}}
  end

  # New data.
  def handle_event(
        :info,
        {kind, socket, bytes},
        {:connected, %ConnectedNode{socket: socket} = connected_node},
        %__MODULE__{} = data
      )
      when kind in [:tcp, :ssl] do
    :ok = inet_mod(data.transport).setopts(socket, active: :once)
    data = update_in(data.buffer, &(&1 <> bytes))
    {data, actions} = consume_new_data(data, connected_node)
    {:keep_state, data, actions}
  end

  # A DBConnection single connection process went up. We don't need to do anything
  def handle_event(:info, {:connected, pid}, {:connected, _node}, %__MODULE__{})
      when is_pid(pid) do
    :keep_state_and_data
  end

  # A DBConnection single connection process disconnected. See `handle_host_health_check_event/3`
  # for details.
  def handle_event(:info, {:disconnected, pid}, {:connected, _node}, %__MODULE__{} = data)
      when is_pid(pid) do
    [{{address, port}, _pool_index}] = Registry.keys(data.registry, pid)
    handle_host_health_check_event(data, address, port)
  end

  # We wait for the pool to register itself before healthcheck
  def handle_event(:info, {:healthcheck, %Host{} = host}, {:connected, _host}, %__MODULE__{}) do
    {:keep_state_and_data,
     {{:timeout, {:check_host_health, Host.to_peername(host)}}, @healthcheck_timeout, nil}}
  end

  # Healthcheck whether a node that we tried to connect is actually up and
  # registered itself. See `handle_host_health_check_event/3` for details
  def handle_event(
        {:timeout, {:check_host_health, {address, port}}},
        nil,
        {:connected, _node},
        %__MODULE__{} = data
      ) do
    handle_host_health_check_event(data, address, port)
  end

  # Used only for testing.
  def handle_event(:cast, {:refresh_topology, peers}, {:connected, _node}, %__MODULE__{} = data) do
    {:keep_state, refresh_topology(data, peers)}
  end

  ## Helper functions

  # We check the registry to see if there are any "up" connections to the same node.
  # If there aren't, we mark the node as down.
  # Eventually, the control connection is going to refresh the cluster topology, if the
  # node is still down but it shows up in the cluster topology, we would be considering
  # it up first and perform another healthcheck, and the cycle continues.
  defp handle_host_health_check_event(data = %__MODULE__{}, address, port) do
    host_info = Map.fetch!(data.peers, {address, port})

    # This match spec was built in IEx using:
    # :ets.fun2ms(fn {{addr_and_port, _}, _, val} when addr_and_port == {{127, 0, 0, 1}, 9042} -> val end)
    spec = [{{{:"$1", :_}, :_, :"$2"}, [{:==, :"$1", {{{address}, port}}}], [:"$2"]}]
    statuses = Registry.select(data.registry, spec)

    if host_info.status == :up and Enum.all?(statuses, &(&1 == :down)) do
      execute_telemetry(data, [:change_event], %{}, %{
        event_type: :host_down,
        host: host_info.host,
        changed: true,
        source: :xandra
      })

      data = update_in(data.lbp, &LoadBalancingPolicy.update_host(&1, host_info.host, :down))
      data = put_in(data.peers[{address, port}].status, :down)
      data = put_in(data.peers[{address, port}].last_seen_at, System.system_time(:millisecond))
      send(data.cluster, {:host_down, host_info.host})
      {:keep_state, data}
    else
      send(data.cluster, {:host_connected, host_info.host})
      :keep_state_and_data
    end
  end

  defp connect_to_first_available_node(%__MODULE__{} = data) do
    {nodes_to_try, data} = nodes_to_try(data)
    connect_to_first_available_node(nodes_to_try, data)
  end

  defp connect_to_first_available_node([], _data) do
    :error
  end

  defp connect_to_first_available_node([%Host{} = host | nodes], data) do
    case connect_to_node({host.address, host.port}, data) do
      {:ok, %ConnectedNode{}, _peers} = return ->
        return

      {:error, reason} ->
        execute_telemetry(data, [:control_connection, :failed_to_connect], %{}, %{
          host: host,
          reason: reason
        })

        connect_to_first_available_node(nodes, data)
    end
  end

  defp connect_to_node({address, port} = node, data) do
    import Utils, only: [request_options: 3]
    %__MODULE__{options: options, transport: transport} = data

    # A nil :protocol_version means "negotiate". A non-nil one means "enforce".
    proto_vsn = Keyword.get(options, :protocol_version)

    case transport.connect(address, port, data.transport_options, @default_timeout) do
      {:ok, socket} ->
        Logger.debug("Connected")
        with {:ok, supported_opts, proto_mod} <- request_options(transport, socket, proto_vsn),
             {:ok, {ip, port}} <- inet_mod(transport).peername(socket),
             connected_node = %ConnectedNode{
               socket: socket,
               protocol_module: proto_mod,
               ip: ip,
               port: port
             },
             :ok <- startup_connection(data, connected_node, supported_opts),
             {:ok, peers} <- fetch_cluster_topology(data, connected_node),
             :ok <- register_to_events(data, connected_node),
             :ok <- inet_mod(transport).setopts(socket, active: :once) do
          [local_host | _] = peers

          res = {:ok, %ConnectedNode{connected_node | host: local_host}, peers}
          Logger.debug("#{inspect(res)}")
          res
        else
          {:error, {:use_this_protocol_instead, _failed_protocol_version, proto_vsn}} ->
            transport.close(socket)
            data = update_in(data.options, &Keyword.put(&1, :protocol_version, proto_vsn))
            connect_to_node(node, data)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # checks if the node is actually up by executing a node ready
  defp attempt_to_connect_to_node({address, port} = node, data) do
    import Utils, only: [request_options: 3]
    %__MODULE__{options: options, transport: transport} = data

    # A nil :protocol_version means "negotiate". A non-nil one means "enforce".
    proto_vsn = Keyword.get(options, :protocol_version)

    Logger.metadata(xandra_address: format_address(address), xandra_port: port)
    Logger.debug("Attempting to connect to #{inspect(node)}")

    with {:ok, socket} <- transport.connect(address, port, data.transport_options, @default_timeout),
         {:ok, supported_opts, proto_mod} <- request_options(transport, socket, proto_vsn),
         {:ok, {ip, port}} <- inet_mod(transport).peername(socket),
         connected_node = %ConnectedNode{
           socket: socket,
           protocol_module: proto_mod,
           ip: ip,
           port: port
         },
         :ok <- startup_connection(data, connected_node, supported_opts)
      do
        Logger.debug("Node #{inspect(node)} is up")
        :ok
    else
      _ ->
        Logger.debug("Node #{inspect(node)} is still down")
        {:error, :timeout}
    end

  end

  defp refresh_topology(%__MODULE__{peers: old_peers} = data, new_peers) do
    old_peers_set = old_peers |> Map.keys() |> MapSet.new()
    new_peers_set = MapSet.new(new_peers, &Host.to_peername/1)

    # Notify cluster of all the peers that got removed.
    Enum.each(MapSet.difference(old_peers_set, new_peers_set), fn peername ->
      %{host: %Host{} = host} = Map.fetch!(old_peers, peername)
      send(data.cluster, {:host_removed, host})

      execute_telemetry(data, [:change_event], %{}, %{
        event_type: :host_removed,
        host: host,
        changed: true,
        source: :xandra
      })
    end)


    {existing_hosts, discovered_hosts, downed_hosts} =
      Enum.reduce(new_peers, {[], [], MapSet.new()}, fn %Host{} = host, {existing_acc, discovered_acc, downed_acc} ->
        peername = Host.to_peername(host)

        case Map.fetch(old_peers, peername) do
          {:ok, %{status: :up}} ->
            {existing_acc ++ [host], discovered_acc, downed_acc}

          {:ok, %{status: :down}} ->
            case attempt_to_connect_to_node({host.address, host.port}, data) do
              :ok ->
                execute_telemetry(data, [:change_event], %{}, %{
                  event_type: :host_up,
                  host: host,
                  changed: true,
                  source: :xandra
                })
                send(data.cluster, {:host_up, host})
                {existing_acc ++ [host], discovered_acc, downed_acc}
              _ ->
                send(data.cluster, {:host_down, host})
                {existing_acc ++ [host], discovered_acc, MapSet.put(downed_acc, peername)}
            end
          :error ->
            execute_telemetry(data, [:change_event], %{}, %{
              event_type: :host_added,
              host: host,
              changed: true,
              source: :xandra
            })

            {existing_acc, discovered_acc ++ [host], downed_acc}
        end
      end)

    if discovered_hosts != [] do
      send(data.cluster, {:discovered_hosts, discovered_hosts})
    end

    final_peers =
      Enum.reduce(existing_hosts ++ discovered_hosts, %{}, fn host, acc ->
        peername = Host.to_peername(host)
        status = case MapSet.member?(downed_hosts, peername) do
          true -> :down
          false -> :up
        end
        Map.put(acc, Host.to_peername(host), %{host: host, status: status, last_seen_at: System.system_time(:millisecond)})
      end)

    data =
      if final_peers != old_peers do
        reset_lbp(data, Enum.map(final_peers, fn {_peername, %{host: host}} -> host end))
      else
        data
      end

    %__MODULE__{data | peers: final_peers}
  end

  defp startup_connection(%__MODULE__{} = data, %ConnectedNode{} = node, supported_options) do
    %{"CQL_VERSION" => [cql_version | _]} = supported_options

    Utils.startup_connection(
      data.transport,
      node.socket,
      _requested_options = %{"CQL_VERSION" => cql_version},
      node.protocol_module,
      _compressor = nil,
      data.options
    )
  end

  defp register_to_events(%__MODULE__{} = data, %ConnectedNode{} = node) do
    payload =
      Frame.new(:register, _options = [])
      |> node.protocol_module.encode_request(["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
      |> Frame.encode(node.protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(node.protocol_module)

    with :ok <- data.transport.send(node.socket, payload),
         {:ok, %Frame{} = frame} <- recv_frame(data.transport, node.socket, protocol_format) do
      :ok = node.protocol_module.decode_response(frame)
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  # Discover the peers in the same data center as the node we're connected to.
  defp fetch_cluster_topology(%__MODULE__{} = data, %ConnectedNode{} = node) do
    # https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useQuerySystemTableCluster.html
    select_peers_query = "SELECT * FROM system.peers"

    select_local_query =
      "SELECT data_center, host_id, rack, release_version, schema_version, tokens FROM system.local"

    with {:ok, [local_node_info]} <- query(data, node, select_local_query),
         {:ok, peers} <- query(data, node, select_peers_query) do
      local_peer = queried_peer_to_host(local_node_info)
      local_peer = %Host{local_peer | address: node.ip, port: node.port}

      Logger.debug("Local peer: #{inspect(local_peer)}")
      Logger.debug("Peers: #{inspect(peers)}")

      Logger.debug("Cluster: #{inspect(data.cluster)}")
      # We filter out the peers with null host_id because they seem to be nodes that are down or
      # decommissioned but not removed from the cluster. See
      # https://github.com/lexhide/xandra/pull/196 and
      # https://user.cassandra.apache.narkive.com/APRtj5hb/system-peers-and-decommissioned-nodes.
      peers =
        for peer_attrs <- peers,
            peer = queried_peer_to_host(peer_attrs),
            peer = %Host{peer | port: data.autodiscovered_nodes_port},
            not is_nil(peer.host_id),
            do: peer

      {:ok, [local_peer | peers]}
    end
  end

  defp same_peer_was_recently_downed?(%__MODULE__{} = data, peer) do
    with {:ok, peer_state} <- Map.fetch(data.peers, peer),
         :down <- peer_state.status,
         last_down_time <- peer_state |> Access.get(:last_seen_at, 0)
      do
        last_down_time + @default_backoff > System.monotonic_time(:millisecond)
    else
      _ -> false
    end
  end

  defp handle_change_event(data, _connected_node, %StatusChange{
         effect: "UP",
         address: address,
         port: port
       }) do
    peer = {address, port}
    %{host: host, status: status} = Map.fetch!(data.peers, peer)

    telemetry_meta = %{event_type: :host_up, source: :cassandra, host: host}

    case status do
      # We already know this peer and we already think it's up, nothing to do.
      :up ->
        execute_telemetry(data, [:change_event], %{}, Map.put(telemetry_meta, :changed, false))
        {data, _actions = []}

      # We already know this peer but we think it's down, so let's mark it as up
      # and notify the cluster.
      :down ->
        execute_telemetry(data, [:change_event], %{}, Map.put(telemetry_meta, :changed, true))
        data = update_in(data.lbp, &LoadBalancingPolicy.update_host(&1, host, :up))
        send(data.cluster, {:host_up, host})
        {put_in(data.peers[peer].status, :up), _actions = []}
    end
  end

  defp handle_change_event(data, _connected_node, %StatusChange{
         effect: "DOWN",
         address: address,
         port: port
       }) do
    peer = {address, port}
    %{host: host, status: status} = Map.fetch!(data.peers, peer)
    telemetry_meta = %{event_type: :host_down, source: :cassandra, host: host}

    case status do
      # We already know this peer and we already think it's down, nothing to do.
      :down ->
        execute_telemetry(data, [:change_event], %{}, Map.put(telemetry_meta, :changed, false))
        {data, _actions = []}

      # We already know this peer but we think it's down, so let's mark it as up
      # and notify the cluster.
      :up ->
        execute_telemetry(data, [:change_event], %{}, Map.put(telemetry_meta, :changed, true))
        data = update_in(data.lbp, &LoadBalancingPolicy.update_host(&1, host, :down))
        send(data.cluster, {:host_down, host})
        data = put_in(data.peers[peer].status, :down)
        data = put_in(data.peers[peer].last_seen_at, System.system_time(:millisecond))
        {data, _actions = []}
    end
  end

  # When we get a NEW_NODE, we need to re-query the system.peers to get info about the new node.
  defp handle_change_event(data, _connected_node, %TopologyChange{effect: "NEW_NODE"} = event) do
    timeout_action = {{:timeout, :delayed_topology_change}, @delay_after_topology_change, event}
    {data, [timeout_action]}
  end

  # If we know about this node, we remove it from the list of nodes and send the event
  # to the cluster. If we don't know about this node, this is a no-op.
  defp handle_change_event(data, _connected_node, %TopologyChange{
         effect: "REMOVED_NODE",
         address: address,
         port: port
       }) do
    telemetry_meta = %{event_type: :host_removed, source: :cassandra}

    case get_and_update_in(data.peers, &Map.pop(&1, {address, port})) do
      {%{host: host}, data} ->
        execute_telemetry(
          data,
          [:change_event],
          %{},
          Map.merge(telemetry_meta, %{host: host, changed: true})
        )

        data = update_in(data.lbp, &LoadBalancingPolicy.update_host(&1, host, :removed))
        send(data.cluster, {:host_removed, host})
        {data, _actions = []}

      {nil, data} ->
        execute_telemetry(
          data,
          [:change_event],
          %{},
          Map.merge(telemetry_meta, %{host: %Host{address: address, port: port}, changed: false})
        )

        {data, _actions = []}
    end
  end

  defp handle_change_event(data, _connected_node, %TopologyChange{effect: "MOVED_NODE"} = event) do
    Logger.warning("Ignored TOPOLOGY_CHANGE event: #{inspect(event)}")
    {data, _actions = []}
  end

  defp consume_new_data(%__MODULE__{} = data, %ConnectedNode{} = connected_node) do
    consume_new_data(data, connected_node, [])
  end

  defp consume_new_data(%__MODULE__{} = data, %ConnectedNode{} = connected_node, actions) do
    fetch_bytes_fun = fn binary, byte_count ->
      case binary do
        <<part::binary-size(byte_count), rest::binary>> -> {:ok, part, rest}
        _other -> {:error, :not_enough_data}
      end
    end

    rest_fun = & &1

    function =
      case connected_node.protocol_module do
        Xandra.Protocol.V5 -> :decode_v5
        Xandra.Protocol.V4 -> :decode_v4
        Xandra.Protocol.V3 -> :decode_v4
      end

    case apply(Xandra.Frame, function, [fetch_bytes_fun, data.buffer, _compressor = nil, rest_fun]) do
      {:ok, frame, rest} ->
        change_event = connected_node.protocol_module.decode_response(frame)
        {data, new_actions} = handle_change_event(data, connected_node, change_event)
        consume_new_data(%__MODULE__{data | buffer: rest}, connected_node, actions ++ new_actions)

      {:error, _reason} ->
        {data, actions}
    end
  end

  defp inet_mod(:gen_tcp), do: :inet
  defp inet_mod(:ssl), do: :ssl

  defp recv_frame(transport, socket, protocol_format) do
    case Utils.recv_frame(transport, socket, protocol_format, _compressor = nil) do
      {:ok, frame, ""} -> {:ok, frame}
      {:error, reason} -> {:error, reason}
    end
  end

  defp query(%__MODULE__{} = data, %ConnectedNode{} = node, statement) do
    query = %Simple{statement: statement, values: [], default_consistency: :one}

    payload =
      Frame.new(:query, _options = [])
      |> node.protocol_module.encode_request(query)
      |> Frame.encode(node.protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(node.protocol_module)

    with :ok <- data.transport.send(node.socket, payload),
         {:ok, %Frame{} = frame} <- recv_frame(data.transport, node.socket, protocol_format) do
      {%Xandra.Page{} = page, _warnings} = node.protocol_module.decode_response(frame, query)
      {:ok, Enum.to_list(page)}
    end
  end

  defp queried_peer_to_host(%{"peer" => _} = peer_attrs) do
    {address, peer_attrs} = Map.pop!(peer_attrs, "peer")
    peer_attrs = Map.put(peer_attrs, "address", address)
    queried_peer_to_host(peer_attrs)
  end

  defp queried_peer_to_host(%{} = peer_attrs) do
    columns = [
      "address",
      "data_center",
      "host_id",
      "rack",
      "release_version",
      "schema_version",
      "tokens"
    ]

    peer_attrs =
      peer_attrs
      |> Map.take(columns)
      |> Enum.map(fn {key, val} -> {String.to_existing_atom(key), val} end)

    struct!(Host, peer_attrs)
  end

  defp contact_points_to_hosts(contact_points) do
    Enum.map(contact_points, fn
      {host, port} ->
        %Host{address: host, port: port}

      contact_point ->
        {:ok, {host, port}} = Xandra.OptionsValidators.validate_node(contact_point)
        %Host{address: host, port: port}
    end)
  end

  # If we have no hosts from the load-balancing policy, we fall back to the contact
  # points (in order). Otherwise, we ignore the contact points and go with the hosts
  # from the load-balancing policy.
  defp nodes_to_try(%__MODULE__{} = data) do
    {hosts, data} = get_and_update_in(data.lbp, &LoadBalancingPolicy.hosts_plan/1)

    if Enum.empty?(hosts) do
      {data.contact_points, data}
    else
      {hosts, data}
    end
  end

  defp reset_lbp(%__MODULE__{lbp: {mod, _state}, lb_opts: lb_opts} = data, new_hosts) do
    state =
      Enum.reduce(new_hosts, mod.init(lb_opts), fn %Host{} = host, acc ->
        mod.host_added(acc, host)
      end)

    %__MODULE__{data | lbp: {mod, state}}
  end

  defp execute_telemetry(%__MODULE__{} = data, event_postfix, measurements, extra_meta) do
    meta = Map.merge(%{cluster_name: data.name, cluster_pid: data.cluster}, extra_meta)
    :telemetry.execute([:xandra, :cluster] ++ event_postfix, measurements, meta)
  end

  defp execute_disconnected_telemetry(%__MODULE__{} = data, %ConnectedNode{host: host}, reason) do
    meta = %{host: host, reason: reason}
    execute_telemetry(data, [:control_connection, :disconnected], %{}, meta)
  end

  # Returns {:error, reason} if the socket was closes or if there was any data
  # coming from the socket. Otherwise, returns :ok.
  defp assert_no_transport_message(socket) do
    receive do
      {kind, ^socket, _data} when kind in [:tcp, :ssl] -> {:error, :data}
      {kind, ^socket, reason} when kind in [:tcp_error, :ssl_error] -> {:error, reason}
      {kind, ^socket} when kind in [:tcp_closed, :ssl_closed] -> {:error, :closed}
    after
      0 -> :ok
    end
  end
end
