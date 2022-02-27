defmodule Xandra.Cluster.ControlConnection do
  @moduledoc false

  @behaviour :gen_statem

  alias Xandra.{Frame, Simple, Connection.Utils}

  require Logger

  @default_backoff 5_000
  @default_timeout 5_000
  @forced_transport_options [packet: :raw, mode: :binary, active: false]

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

  def start_link(cluster, node_ref, address, port, options, autodiscovery?) do
    transport_options =
      options
      |> Keyword.get(:transport_options, [])
      |> Keyword.merge(@forced_transport_options)

    data = %__MODULE__{
      cluster: cluster,
      node_ref: node_ref,
      address: address,
      port: port,
      options: options,
      transport: if(options[:encryption], do: :ssl, else: :gen_tcp),
      transport_options: transport_options,
      autodiscovery: autodiscovery?
    }

    :gen_statem.start_link(__MODULE__, data, [])
  end

  ## Callbacks

  @impl :gen_statem
  def init(data) do
    {:ok, :disconnected, data, {:next_event, :internal, :connect}}
  end

  @impl :gen_statem
  def callback_mode do
    :state_functions
  end

  # Disconnected state

  def disconnected(:internal, :connect, %__MODULE__{} = data) do
    %__MODULE__{options: options, address: address, port: port, transport: transport} = data

    protocol_mod = Keyword.fetch!(options, :protocol_module)
    data = %__MODULE__{data | protocol_module: protocol_mod}

    case transport.connect(address, port, data.transport_options, @default_timeout) do
      {:ok, socket} ->
        data = %__MODULE__{data | socket: socket}

        with {:ok, supported_options} <- Utils.request_options(transport, socket, protocol_mod),
             :ok <-
               startup_connection(transport, socket, supported_options, protocol_mod, options),
             {:ok, peers_or_nil} <-
               maybe_discover_peers(data.autodiscovery, transport, socket, protocol_mod),
             :ok <- register_to_events(transport, socket, protocol_mod),
             :ok <- inet_mod(transport).setopts(socket, active: true) do
          {:ok, data} = report_active(data)

          if not is_nil(peers_or_nil) do
            report_peers(data, peers_or_nil)
          end

          {:next_state, :connected, data}
        else
          {:error, _reason} = error ->
            {:connect, :reconnect, data} = disconnect(error, data)
            timeout_action = {{:timeout, :reconnect}, @default_backoff, data}
            {:keep_state, data, timeout_action}
        end

      {:error, _reason} ->
        timeout_action = {{:timeout, :reconnect}, @default_backoff, data}
        {:keep_state_and_data, timeout_action}
    end
  end

  def disconnected(:info, {kind, socket, _other}, %__MODULE__{socket: socket})
      when kind in [:tcp_error, :ssl_error] do
    :keep_state_and_data
  end

  def disconnected(:info, {kind, socket}, %__MODULE__{socket: socket})
      when kind in [:tcp_closed, :ssl_closed] do
    :keep_state_and_data
  end

  def disconnect({:error, _reason}, %__MODULE__{} = state) do
    state.transport.close(state.socket)
    {:connect, :reconnect, %{state | socket: nil, buffer: <<>>}}
  end

  # Connected state

  def connected(:info, {kind, socket, _reason}, %__MODULE__{socket: socket} = data)
      when kind in [:tcp_error, :ssl_error] do
    data.transport.close(data.socket)
    data = %__MODULE__{data | buffer: <<>>}
    {:next_state, :disconnected, data, {:next_event, :internal, :connect}}
  end

  def connected(:info, {kind, socket}, %__MODULE__{socket: socket} = data)
      when kind in [:tcp_closed, :ssl_closed] do
    data.transport.close(data.socket)
    data = %__MODULE__{data | buffer: <<>>, socket: nil}
    {:next_state, :disconnected, data, {:next_event, :internal, :connect}}
  end

  def connected({kind, socket, bytes}, %__MODULE__{socket: socket, buffer: buffer} = data)
      when kind in [:tcp, :ssl] do
    data = %__MODULE__{data | buffer: buffer <> bytes}
    data = report_event(data)
    {:keep_state, data}
  end

  ## Helper functions

  defp report_active(%__MODULE__{new: false, cluster: cluster, peername: peername} = data) do
    Xandra.Cluster.update(cluster, {:control_connection_established, peername})
    {:ok, data}
  end

  defp report_active(
         %__MODULE__{new: true, cluster: cluster, node_ref: node_ref, socket: socket} = data
       ) do
    with {:ok, {address, port}} <- inet_mod(data.transport).peername(socket) do
      Xandra.Cluster.activate(cluster, node_ref, address, port)
      {:ok, %{data | new: false, peername: address}}
    end
  end

  defp report_peers(state, peers) do
    :ok = Xandra.Cluster.discovered_peers(state.cluster, peers)
  end

  defp startup_connection(transport, socket, supported_options, protocol_module, options) do
    %{"CQL_VERSION" => [cql_version | _]} = supported_options
    requested_options = %{"CQL_VERSION" => cql_version}
    Utils.startup_connection(transport, socket, requested_options, protocol_module, nil, options)
  end

  defp register_to_events(transport, socket, protocol_module) do
    payload =
      Frame.new(:register)
      |> protocol_module.encode_request(["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
      |> Frame.encode(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <- Utils.recv_frame(transport, socket, protocol_module) do
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
      Frame.new(:query)
      |> protocol_module.encode_request(query)
      |> Frame.encode(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <- Utils.recv_frame(transport, socket, protocol_module) do
      %Xandra.Page{} = page = protocol_module.decode_response(frame, query)
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
      Frame.new(:query)
      |> protocol_module.encode_request(query)
      |> Frame.encode(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <- Utils.recv_frame(transport, socket, protocol_module) do
      %Xandra.Page{} = page = protocol_module.decode_response(frame, query)
      {:ok, Enum.to_list(page)}
    end
  end

  defp report_event(%{cluster: cluster, buffer: buffer} = state) do
    case decode_frame(buffer, state.protocol_module) do
      {frame, rest} ->
        change_event = state.protocol_module.decode_response(frame)
        Logger.debug("Received event: #{inspect(change_event)}")
        Xandra.Cluster.update(cluster, change_event)
        report_event(%{state | buffer: rest})

      :error ->
        state
    end
  end

  defp decode_frame(buffer, protocol_module) do
    header_length = Frame.header_length()

    case buffer do
      <<header::size(header_length)-bytes, rest::binary>> ->
        body_length = Frame.body_length(header)

        case rest do
          <<body::size(body_length)-bytes, rest::binary>> ->
            {Frame.decode(header, body, protocol_module), rest}

          _ ->
            :error
        end

      _ ->
        :error
    end
  end

  defp inet_mod(:gen_tcp), do: :inet
  defp inet_mod(:ssl), do: :ssl
end
