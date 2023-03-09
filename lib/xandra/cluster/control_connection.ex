defmodule Xandra.Cluster.ControlConnection do
  @moduledoc false

  @behaviour :gen_statem

  alias Xandra.{Frame, Simple, Cluster, Connection.Utils}

  require Logger

  @default_backoff 5_000
  @default_timeout 5_000
  @forced_transport_options [packet: :raw, mode: :binary, active: false]

  # Internal NimbleOptions schema used to validate the options given to start_link/1.
  # This is only used for internal consistency and having an additional layer of
  # weak "type checking" (some people might get angry at this).
  @opts_schema NimbleOptions.new!(
                 cluster: [type: :pid, required: true],
                 node_ref: [type: :reference, required: true],
                 address: [type: :any, required: true],
                 port: [type: {:in, 0..65355}, required: true],
                 connection_options: [type: :keyword_list, required: true],
                 autodiscovery: [type: :boolean, required: true]
               )

  defstruct [
    :cluster,
    :node_ref,
    :address,
    :port,
    :transport,
    :transport_options,
    :socket,
    :options,
    :autodiscovery,
    :protocol_module,
    :peername,
    new: true,
    buffer: <<>>
  ]

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

    transport_options =
      connection_options
      |> Keyword.get(:transport_options, [])
      |> Keyword.merge(@forced_transport_options)

    data = %__MODULE__{
      cluster: Keyword.fetch!(options, :cluster),
      node_ref: Keyword.fetch!(options, :node_ref),
      address: Keyword.fetch!(options, :address),
      port: Keyword.fetch!(options, :port),
      autodiscovery: Keyword.fetch!(options, :autodiscovery),
      options: connection_options,
      transport: transport,
      transport_options: transport_options
    }

    :gen_statem.start_link(__MODULE__, data, [])
  end

  ## Callbacks

  @impl :gen_statem
  def init(data) do
    Logger.debug("Started control connection process (#{inspect(data.address)})")
    {:ok, :disconnected, data, {:next_event, :internal, :connect}}
  end

  @impl :gen_statem
  def callback_mode do
    :state_functions
  end

  # Disconnected state

  def disconnected(:internal, :connect, %__MODULE__{} = data) do
    %__MODULE__{options: options, address: address, port: port, transport: transport} = data

    # A nil :protocol_version means "negotiate". A non-nil one means "enforce".
    protocol_version = Keyword.get(options, :protocol_version)

    case transport.connect(address, port, data.transport_options, @default_timeout) do
      {:ok, socket} ->
        data = %__MODULE__{data | socket: socket}

        with {:ok, supported_options, protocol_module} <-
               Utils.request_options(transport, socket, protocol_version),
             Logger.debug("Supported options: #{inspect(supported_options)}"),
             data = %__MODULE__{data | protocol_module: protocol_module},
             :ok <-
               startup_connection(transport, socket, supported_options, protocol_module, options),
             {:ok, peers_or_nil} <-
               maybe_discover_peers(data.autodiscovery, transport, socket, protocol_module),
             :ok <- register_to_events(transport, socket, protocol_module),
             :ok <- inet_mod(transport).setopts(socket, active: true) do
          Logger.debug("Established control connection (protocol #{inspect(protocol_module)})")
          {:ok, data} = report_active(data)

          if not is_nil(peers_or_nil) do
            report_peers(data, peers_or_nil)
          end

          {:next_state, :connected, data}
        else
          {:error, {:use_this_protocol_instead, _failed_protocol_version, protocol_version}} ->
            :ok = transport.close(socket)
            data = update_in(data.options, &Keyword.put(&1, :protocol_version, protocol_version))
            {:keep_state, data, {:next_event, :internal, :connect}}

          {:error, %Xandra.Error{} = error} ->
            Logger.error(
              "Failed to establish control connection because of Cassandra error: " <>
                Exception.message(error)
            )

            {:stop, error}

          {:error, %Xandra.ConnectionError{}} = error ->
            {:connect, :reconnect, data} = disconnect(error, data)
            timeout_action = {{:timeout, :reconnect}, @default_backoff, nil}
            {:keep_state, data, timeout_action}
        end

      {:error, reason} ->
        Logger.debug(
          "Failed to connect to #{inspect(address)}:#{port}: #{:inet.format_error(reason)}"
        )

        timeout_action = {{:timeout, :reconnect}, @default_backoff, data}
        {:keep_state_and_data, timeout_action}
    end
  end

  # TCP/SSL messages that we get when we're already in the "disconnected" state can
  # be safely ignored.
  def disconnected(:info, {kind, socket, _reason}, %__MODULE__{socket: socket})
      when kind in [:tcp_error, :ssl_error] do
    :keep_state_and_data
  end

  # TCP/SSL messages that we get when we're already in the "disconnected" state can
  # be safely ignored.
  def disconnected(:info, {kind, socket}, %__MODULE__{socket: socket})
      when kind in [:tcp_closed, :ssl_closed] do
    :keep_state_and_data
  end

  def disconnected({:timeout, :reconnect}, _content, _data) do
    {:keep_state_and_data, {:next_event, :internal, :connect}}
  end

  # Connected state

  def connected(:info, {kind, socket, reason}, %__MODULE__{socket: socket} = data)
      when kind in [:tcp_error, :ssl_error] do
    Logger.debug("Socket error: #{inspect(reason)}")
    {:connect, :reconnect, data} = disconnect({:error, reason}, data)
    {:next_state, :disconnected, data, {:next_event, :internal, :connect}}
  end

  def connected(:info, {kind, socket}, %__MODULE__{socket: socket} = data)
      when kind in [:tcp_closed, :ssl_closed] do
    Logger.debug("Socket closed")
    data.transport.close(data.socket)
    data = %__MODULE__{data | buffer: <<>>, socket: nil}
    {:next_state, :disconnected, data, {:next_event, :internal, :connect}}
  end

  def connected(:info, {kind, socket, bytes}, %__MODULE__{socket: socket} = data)
      when kind in [:tcp, :ssl] do
    data = update_in(data.buffer, &(&1 <> bytes))
    data = consume_new_data(data)
    {:keep_state, data}
  end

  ## Helper functions

  defp disconnect({:error, reason}, %__MODULE__{} = data) do
    Logger.debug(
      "Disconnecting from #{address_to_human_readable_source(data)} because of error: #{:inet.format_error(reason)}"
    )

    _ = data.transport.close(data.socket)
    {:connect, :reconnect, %__MODULE__{data | socket: nil, buffer: <<>>}}
  end

  # A control connection that never came online just came online.
  defp report_active(
         %__MODULE__{new: true, cluster: cluster, node_ref: node_ref, socket: socket} = data
       ) do
    case inet_mod(data.transport).peername(socket) do
      {:ok, {_ip, _port} = peername} ->
        :ok = Cluster.activate(cluster, node_ref, peername)
        data = %__MODULE__{data | new: false, peername: peername}
        {:ok, data}
    end
  end

  defp report_active(%__MODULE__{new: false, cluster: cluster, peername: peername} = data) do
    Xandra.Cluster.update(cluster, {:control_connection_established, peername})
    {:ok, data}
  end

  defp report_peers(state, peers) do
    source = address_to_human_readable_source(state)
    :ok = Xandra.Cluster.discovered_peers(state.cluster, peers, source)
  end

  defp startup_connection(transport, socket, supported_options, protocol_module, options) do
    %{"CQL_VERSION" => [cql_version | _]} = supported_options
    requested_options = %{"CQL_VERSION" => cql_version}
    Utils.startup_connection(transport, socket, requested_options, protocol_module, nil, options)
  end

  defp register_to_events(transport, socket, protocol_module) do
    payload =
      Frame.new(:register, _options = [])
      |> protocol_module.encode_request(["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <-
           Utils.recv_frame(transport, socket, protocol_format, _compressor = nil) do
      :ok = protocol_module.decode_response(frame)
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp maybe_discover_peers(_autodiscovery? = false, _transport, _socket, _protocol_module) do
    {:ok, _peers = nil}
  end

  defp maybe_discover_peers(_autodiscovery? = true, transport, socket, protocol_module) do
    # Discover the peers in the same data center as the node we're connected to.
    with {:ok, local_info} <- fetch_node_local_info(transport, socket, protocol_module),
         local_data_center = Map.fetch!(local_info, "data_center"),
         {:ok, peers} <- discover_peers(transport, socket, protocol_module) do
      # We filter out the peers with null host_id because they seem to be nodes that are down or
      # decommissioned but not removed from the cluster. See
      # https://github.com/lexhide/xandra/pull/196 and
      # https://user.cassandra.apache.narkive.com/APRtj5hb/system-peers-and-decommissioned-nodes.
      peers =
        for %{"host_id" => host_id, "data_center" => data_center, "rpc_address" => address} <-
              peers,
            not is_nil(host_id),
            data_center == local_data_center,
            do: address

      {:ok, peers}
    end
  end

  defp fetch_node_local_info(transport, socket, protocol_module) do
    query = %Simple{
      statement: "SELECT data_center FROM system.local",
      values: [],
      default_consistency: :one
    }

    payload =
      Frame.new(:query, _options = [])
      |> protocol_module.encode_request(query)
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <-
           Utils.recv_frame(transport, socket, protocol_format, _compressor = nil) do
      {%Xandra.Page{} = page, _warnings} = protocol_module.decode_response(frame, query)
      [local_info] = Enum.to_list(page)
      {:ok, local_info}
    end
  end

  defp discover_peers(transport, socket, protocol_module) do
    query = %Simple{
      statement: "SELECT host_id, rpc_address, data_center FROM system.peers",
      values: [],
      default_consistency: :one
    }

    payload =
      Frame.new(:query, _options = [])
      |> protocol_module.encode_request(query)
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <-
           Utils.recv_frame(transport, socket, protocol_format, _compressor = nil) do
      {%Xandra.Page{} = page, _warnings} = protocol_module.decode_response(frame, query)
      {:ok, Enum.to_list(page)}
    end
  end

  defp consume_new_data(%__MODULE__{cluster: cluster} = data) do
    case decode_frame(data.buffer) do
      {frame, rest} ->
        {change_event, _warnings} = data.protocol_module.decode_response(frame)
        Logger.debug("Received event: #{inspect(change_event)}")
        :ok = Cluster.update(cluster, change_event)
        consume_new_data(%__MODULE__{data | buffer: rest})

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

  defp address_to_human_readable_source(%__MODULE__{peername: {ip, port}}),
    do: "#{:inet.ntoa(ip)}:#{port}"

  defp address_to_human_readable_source(%__MODULE__{address: {_, _, _, _} = address, port: port}),
    do: "#{:inet.ntoa(address)}:#{port}"

  defp address_to_human_readable_source(%__MODULE__{address: address, port: port}),
    do: "#{address}:#{port}"
end
