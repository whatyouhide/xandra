defmodule Xandra.Cluster.ControlConnection do
  @moduledoc false

  @behaviour :gen_statem

  alias Xandra.{Frame, Simple, Connection.Utils}

  require Logger

  @default_backoff 5_000
  @default_timeout 5_000
  @forced_transport_options [packet: :raw, mode: :binary, active: false]

  # Internal NimbleOptions schema used to validate the options given to start_link/1.
  # This is only used for internal consistency and having an additional layer of
  # weak "type checking" (some people might get angry at this).
  @opts_schema NimbleOptions.new!(
                 cluster: [type: :pid, required: true],
                 contact_points: [type: :any, required: true],
                 connection_options: [type: :keyword_list, required: true],
                 autodiscovered_nodes_port: [type: :non_neg_integer, required: true]
               )

  defstruct [
    # The PID of the parent cluster.
    :cluster,

    # A list of {host, port} tuples. This is a mix of the seed nodes provided by the user
    # as well as the nodes discovered throughout the lifetime of the control connection.
    :peers,

    # The transport and its options, used to connect to the provided nodes.
    :transport,
    :transport_options,

    # The options to use to connect to the nodes.
    :options,

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
      |> Enum.map(fn
        {host, port} ->
          {host, port}

        contact_point ->
          {:ok, node} = Xandra.OptionsValidators.validate_node(contact_point)
          node
      end)

    data = %__MODULE__{
      cluster: Keyword.fetch!(options, :cluster),
      peers: contact_points,
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
      {:ok, connected_node, peers} ->
        peers = Enum.uniq([{connected_node.ip, connected_node.port} | peers])
        send(data.cluster, {:discovered_peers, peers})

        data = %__MODULE__{data | peers: Enum.uniq(data.peers ++ peers)}
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

  ## Helper functions

  defp connect_to_first_available_node(%__MODULE__{} = data) do
    Enum.each(data.peers, fn {_host_or_ip, _port} = peer ->
      case connect_to_node(peer, data) do
        {:ok, %ConnectedNode{ip: ip, port: port, protocol_module: proto_mod}, _peers} = return ->
          Logger.metadata(peer: peername_to_string({ip, port}))
          Logger.debug("Established control connection to node (protocol #{inspect(proto_mod)})")
          throw(return)

        {:error, reason} ->
          Logger.warning(
            "Error connecting to #{peername_to_string(peer)}: #{:inet.format_error(reason)}"
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
             connected_node = %ConnectedNode{socket: socket, protocol_module: proto_mod},
             :ok <- startup_connection(data, connected_node, supported_opts),
             {:ok, peers} <- discover_peers(data, connected_node),
             :ok <- register_to_events(data, connected_node),
             :ok <- inet_mod(transport).setopts(socket, active: true) do
          {:ok, {ip, port}} = inet_mod(transport).peername(socket)
          connected_node = %ConnectedNode{connected_node | ip: ip, port: port}
          {:ok, connected_node, peers}
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
    select_peers_query = "SELECT host_id, rpc_address, data_center FROM system.peers"

    with {:ok, local_node_info} <- query(data, node, "SELECT data_center FROM system.local"),
         [%{"data_center" => local_data_center}] = local_node_info,
         {:ok, peers} <- query(data, node, select_peers_query) do
      # We filter out the peers with null host_id because they seem to be nodes that are down or
      # decommissioned but not removed from the cluster. See
      # https://github.com/lexhide/xandra/pull/196 and
      # https://user.cassandra.apache.narkive.com/APRtj5hb/system-peers-and-decommissioned-nodes.
      peers =
        for %{"host_id" => host_id, "data_center" => dc, "rpc_address" => address} <- peers,
            not is_nil(host_id),
            dc == local_data_center,
            do: address

      {:ok, peers}
    end
  end

  defp consume_new_data(%__MODULE__{cluster: cluster} = data, %ConnectedNode{} = connected_node) do
    case decode_frame(data.buffer) do
      {frame, rest} ->
        {change_event, _warnings} = connected_node.protocol_module.decode_response(frame)
        Logger.debug("Received event: #{inspect(change_event)}")
        send(cluster, {:cluster_event, change_event})
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
end
