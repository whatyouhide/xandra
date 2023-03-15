defmodule Xandra.Cluster.ControlConnection do
  @moduledoc false

  @behaviour :gen_statem

  alias Xandra.{Frame, Simple, Connection.Utils}
  alias Xandra.Cluster.{Host, StatusChange, TopologyChange}

  require Logger

  @default_backoff 5_000
  @default_timeout 5_000
  @forced_transport_options [packet: :raw, mode: :binary, active: false]

  # Internal NimbleOptions schema used to validate the options given to start_link/1.
  # This is only used for internal consistency and having an additional layer of
  # weak "type checking" (some people might get angry at this).
  @opts_schema [
    cluster: [type: :pid, required: true],
    contact_points: [type: :any, required: true],
    connection_options: [type: :keyword_list, required: true],
    autodiscovered_nodes_port: [type: :non_neg_integer, required: true],
    load_balancing_module: [type: :atom, required: true]
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

    # The load balancing module and state.
    :lb_module,
    :lb_state,

    # A map of {ip, port} => %{host: %Host{}, status: atom}.
    peers: %{},

    # Data buffer.
    buffer: <<>>
  ]

  defmodule ConnectedNode do
    @moduledoc false
    defstruct [:socket, :protocol_module, :ip, :port]
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

    data = %__MODULE__{
      cluster: Keyword.fetch!(options, :cluster),
      contact_points: contact_points,
      autodiscovered_nodes_port: Keyword.fetch!(options, :autodiscovered_nodes_port),
      lb_module: Keyword.fetch!(options, :load_balancing_module),
      lb_state: Keyword.fetch!(options, :load_balancing_module).init(contact_points),
      options: connection_options,
      transport: transport,
      transport_options: Keyword.merge(transport_options, @forced_transport_options)
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
    Logger.debug("Started control connection process")
    {:ok, :disconnected, data, {:next_event, :internal, :connect}}
  end

  @impl :gen_statem
  def callback_mode, do: :handle_event_function

  # Disconnected state

  @impl true
  def handle_event(type, content, state, data)

  # Connecting is the hardest thing control connections do. The gist is this:
  #
  #   1. We try to connect to each node in :seed_peernames until one succeeds
  #   2. We discover the peers for that node
  #   3. We register to the events for that node
  #   4. We send the discovered peers back to the cluster alongside the connected node
  #   5. We move to the state {:connected, node}
  def handle_event(:internal, :connect, :disconnected, %__MODULE__{} = data) do
    case connect_to_first_available_node(data) do
      {:ok, connected_node, local_peer, peers} ->
        peers = [local_peer | peers]
        send(data.cluster, {:discovered_peers, peers})

        data =
          Enum.reduce(peers, data, fn %Host{} = host, data ->
            update_in(data.lb_state, &data.lb_module.host_added(&1, host))
          end)

        data =
          put_in(
            data.peers,
            Map.new(peers, &{{&1.address, &1.port}, %{host: &1, status: :up}})
          )

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

  # Trigger the reconnect event once the timer expires.
  def handle_event({:timeout, :reconnect}, _content, :disconnected, _data) do
    {:keep_state_and_data, {:next_event, :internal, :connect}}
  end

  # Connected state

  # If there's a socket error with the current node, we TODO.
  def handle_event(
        :info,
        {kind, socket, reason},
        {:connected, %ConnectedNode{socket: socket}},
        %__MODULE__{} = data
      )
      when kind in [:tcp_error, :ssl_error] do
    _ = data.transport.close(socket)
    Logger.debug("Socket error: #{:inet.format_error(reason)}")
    {:next_state, :disconnected, data, {:next_event, :internal, :connect}}
  end

  # If the current node closes the socket, we TODO
  def handle_event(
        :info,
        {kind, socket},
        {:connected, %ConnectedNode{socket: socket}},
        %__MODULE__{} = data
      )
      when kind in [:tcp_closed, :ssl_closed] do
    _ = data.transport.close(socket)
    Logger.debug("Socket closed")
    Logger.metadata(peer: nil)

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
    data = update_in(data.buffer, &(&1 <> bytes))
    data = consume_new_data(data, connected_node)
    {:keep_state, data}
  end

  # This is a hack. We don't have code to encode EVENT frames to test this properly,
  # so for now we're just cheating.
  # TODO: make me a good piece of code please!
  def handle_event(
        :info,
        {:__test_event__, %_{} = event},
        {:connected, connected_node},
        %__MODULE__{} = data
      ) do
    data = handle_change_event(data, connected_node, event)
    {:keep_state, data}
  end

  ## Helper functions

  defp connect_to_first_available_node(%__MODULE__{} = data) do
    {nodes_to_try, data} = get_and_update_in(data.lb_state, &data.lb_module.hosts_plan(&1))

    Enum.each(nodes_to_try, fn %Host{} = host ->
      case connect_to_node({host.address, host.port}, data) do
        {:ok, %ConnectedNode{ip: ip, port: port, protocol_module: proto_mod}, _local_peer, _peers} =
            return ->
          Logger.metadata(peer: peername_to_string({ip, port}))
          Logger.debug("Established control connection to node (protocol #{inspect(proto_mod)})")
          throw(return)

        {:error, reason} ->
          log_warn(
            "Error connecting to #{peername_to_string({host.address, host.port})}: #{:inet.format_error(reason)}"
          )
      end
    end)

    :error
  catch
    :throw, return -> return
  end

  defp connect_to_node({address, port} = node, data) do
    import Utils, only: [request_options: 3]
    %__MODULE__{options: options, transport: transport} = data

    # A nil :protocol_version means "negotiate". A non-nil one means "enforce".
    proto_vsn = Keyword.get(options, :protocol_version)

    Logger.metadata(peer: peername_to_string(node))
    Logger.debug("Opening new connection")

    case transport.connect(address, port, data.transport_options, @default_timeout) do
      {:ok, socket} ->
        with {:ok, supported_opts, proto_mod} <- request_options(transport, socket, proto_vsn),
             Logger.debug("Supported options: #{inspect(supported_opts)}"),
             {:ok, {ip, port}} <- inet_mod(transport).peername(socket),
             connected_node = %ConnectedNode{
               socket: socket,
               protocol_module: proto_mod,
               ip: ip,
               port: port
             },
             :ok <- startup_connection(data, connected_node, supported_opts),
             {:ok, local_peer, peers} <- discover_peers(data, connected_node),
             :ok <- register_to_events(data, connected_node),
             :ok <- inet_mod(transport).setopts(socket, active: true) do
          {:ok, connected_node, local_peer, peers}
        else
          {:error, {:use_this_protocol_instead, _failed_protocol_version, proto_vsn}} ->
            Logger.debug("Cassandra said to use protocol #{inspect(proto_vsn)}, reconnecting")
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
  defp discover_peers(%__MODULE__{} = data, %ConnectedNode{} = node) do
    # https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useQuerySystemTableCluster.html
    # Columns:
    # peer | data_center | host_id | preferred_ip | rack | release_version | rpc_address | schema_version | tokens
    select_peers_query =
      "SELECT peer, data_center, host_id, rack, release_version, schema_version, tokens FROM system.peers"

    select_local_query =
      "SELECT data_center, host_id, rack, release_version, schema_version, tokens FROM system.local"

    with {:ok, [local_node_info]} <- query(data, node, select_local_query),
         {:ok, peers} <- query(data, node, select_peers_query) do
      local_peer = queried_peer_to_host(local_node_info)
      local_peer = %Host{local_peer | address: node.ip, port: node.port}

      # We filter out the peers with null host_id because they seem to be nodes that are down or
      # decommissioned but not removed from the cluster. See
      # https://github.com/lexhide/xandra/pull/196 and
      # https://user.cassandra.apache.narkive.com/APRtj5hb/system-peers-and-decommissioned-nodes.
      peers =
        for peer_attrs <- peers,
            peer = queried_peer_to_host(peer_attrs),
            peer = %Host{peer | port: data.autodiscovered_nodes_port},
            not is_nil(peer.host_id),
            peer.data_center == local_peer.data_center,
            do: peer

      {:ok, local_peer, peers}
    end
  end

  defp handle_change_event(data, _connected_node, %StatusChange{
         effect: "UP",
         address: address,
         port: port
       }) do
    peer = {address, port}

    case data.peers do
      # We already know this peer and we already think it's up, nothing to do.
      %{^peer => %{status: :up}} ->
        data

      # We already know this peer but we think it's down, so let's mark it as up
      # and notify the cluster.
      %{^peer => %{status: :down, host: host}} ->
        data = update_in(data.lb_state, &data.lb_module.host_up(&1, host))
        send(data.cluster, {:host_up, host})
        put_in(data.peers[peer].status, :up)
    end
  end

  defp handle_change_event(data, _connected_node, %StatusChange{
         effect: "DOWN",
         address: address,
         port: port
       }) do
    peer = {address, port}

    case data.peers do
      # We already know this peer and we already think it's down, nothing to do.
      %{^peer => %{status: :down}} ->
        data

      # We already know this peer but we think it's down, so let's mark it as up
      # and notify the cluster.
      %{^peer => %{status: :up, host: host}} ->
        data = update_in(data.lb_state, &data.lb_module.host_down(&1, host))
        send(data.cluster, {:host_down, host})
        put_in(data.peers[peer].status, :down)
    end
  end

  # When we get a NEW_NODE, we need to re-query the system.peers to get info about the new node.
  defp handle_change_event(data, connected_node, %TopologyChange{
         effect: "NEW_NODE",
         address: address
       }) do
    # TODO: make this a supported API, we need to wait a sec before querying according to other
    # drivers.
    Process.sleep(500)

    select_peers_query =
      "SELECT peer, data_center, host_id, rack, release_version, schema_version, tokens FROM system.peers"

    with {:ok, peers} <- query(data, connected_node, select_peers_query),
         host when not is_nil(host) <- Enum.find(peers, fn peer -> peer["peer"] == address end) do
      new_host = queried_peer_to_host(host)
      new_host = %Host{new_host | port: data.autodiscovered_nodes_port}
      data = update_in(data.lb_state, &data.lb_module.host_added(&1, new_host))
      send(data.cluster, {:host_added, new_host})
      put_in(data.cluster[{new_host.address, new_host.port}], host)
    else
      _ -> data
    end
  end

  # If we know about this node, we remove it from the list of nodes and send the event
  # to the cluster. If we don't know about this node, this is a no-op.
  defp handle_change_event(data, _connected_node, %TopologyChange{
         effect: "REMOVED_NODE",
         address: address,
         port: port
       }) do
    case get_and_update_in(data.peers, &Map.pop(&1, {address, port})) do
      {%{host: host}, data} ->
        data = update_in(data.lb_state, &data.lb_module.host_removed(&1, host))
        send(data.cluster, {:host_removed, host})
        data

      {nil, data} ->
        data
    end
  end

  defp handle_change_event(data, _connected_node, %TopologyChange{effect: "MOVED_NODE"} = event) do
    Logger.warn("Ignored TOPOLOGY_CHANGE event: #{inspect(event)}")
    data
  end

  defp consume_new_data(%__MODULE__{} = data, %ConnectedNode{} = connected_node) do
    case decode_frame(data.buffer) do
      {frame, rest} ->
        {change_event, _warnings} = connected_node.protocol_module.decode_response(frame)
        Logger.debug("Received event: #{inspect(change_event)}")
        data = handle_change_event(data, connected_node, change_event)
        consume_new_data(%__MODULE__{data | buffer: rest}, connected_node)

      :error ->
        data
    end
  end

  defp decode_frame(buffer) do
    header_length = Frame.header_length()

    case buffer do
      <<header::size(header_length)-bytes, rest::binary>> ->
        body_length = Frame.body_length(header)

        case rest do
          <<body::size(body_length)-bytes, rest::binary>> -> {Frame.decode(header, body), rest}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp inet_mod(:gen_tcp), do: :inet
  defp inet_mod(:ssl), do: :ssl

  defp peername_to_string({host_or_ip, port}) do
    if ip_address?(host_or_ip) do
      "#{:inet.ntoa(host_or_ip)}:#{port}"
    else
      "#{host_or_ip}:#{port}"
    end
  end

  # TODO: remove the conditional once we depend on OTP 25+.
  if function_exported?(:inet, :is_ip_address, 1) do
    defp ip_address?(term), do: :inet.is_ip_address(term)
  else
    defp ip_address?(term), do: is_tuple(term)
  end

  defp recv_frame(transport, socket, protocol_format) do
    Utils.recv_frame(transport, socket, protocol_format, _compressor = nil)
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
    peer_attrs = Enum.map(peer_attrs, fn {key, val} -> {String.to_existing_atom(key), val} end)
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

  # TODO: use Logger.warning/2 directly when we depend on Elixir 1.11+.
  if macro_exported?(Logger, :warning, 2) do
    defp log_warn(message, metadata \\ []), do: Logger.log(:warning, message, metadata)
  else
    defp log_warn(message, metadata \\ []), do: Logger.log(:warn, message, metadata)
  end
end
