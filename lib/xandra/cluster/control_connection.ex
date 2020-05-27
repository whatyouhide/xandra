defmodule Xandra.Cluster.ControlConnection do
  @moduledoc false

  use Connection

  alias Xandra.{Frame, Simple, Connection.Utils, Cluster}

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
    :discover_peers,
    new: true,
    buffer: <<>>
  ]

  def start_link(cluster, node_ref, address, port, options, autodiscovery?) do
    transport_options =
      options
      |> Keyword.get(:transport_options, [])
      |> Keyword.merge(@forced_transport_options)

    state = %__MODULE__{
      cluster: cluster,
      node_ref: node_ref,
      address: address,
      port: port,
      options: options,
      transport: if(options[:encryption], do: :ssl, else: :gen_tcp),
      transport_options: transport_options,
      autodiscovery: autodiscovery?
    }

    Connection.start_link(__MODULE__, state)
  end

  def init(state) do
    {:connect, :init, state}
  end

  def connect(_action, %__MODULE__{address: address, port: port, options: options} = state) do
    protocol_module = Keyword.fetch!(options, :protocol_module)
    state = %{state | protocol_module: protocol_module}

    case state.transport.connect(address, port, state.transport_options, @default_timeout) do
      {:ok, socket} ->
        state = %{state | socket: socket}
        transport = state.transport

        with {:ok, supported_options} <-
               Utils.request_options(state.transport, socket, protocol_module),
             :ok <-
               startup_connection(
                 state.transport,
                 socket,
                 supported_options,
                 protocol_module,
                 options
               ),
             {:ok, local_node} <- fetch_node_local_info(transport, socket, protocol_module),
             {:ok, peers_or_nil} <-
               maybe_discover_peers(
                 state.autodiscovery,
                 local_node,
                 transport,
                 socket,
                 protocol_module
               ),
             :ok <- register_to_events(state.transport, socket, protocol_module),
             :ok <- inet_mod(state.transport).setopts(socket, active: true) do
          {:ok, state} = report_active(state, local_node)

          if not is_nil(peers_or_nil) do
            report_peers(state, peers_or_nil)
          end

          {:ok, %{state | discover_peers: peers_or_nil}}
        else
          {:error, _reason} = error ->
            {:connect, :reconnect, state} = disconnect(error, state)
            {:backoff, @default_backoff, state}
        end

      {:error, _reason} ->
        {:backoff, @default_backoff, state}
    end
  end

  def handle_info({kind, socket, reason}, %__MODULE__{socket: socket} = state)
      when kind in [:tcp_error, :ssl_error] do
    {:disconnect, {:error, reason}, state}
  end

  def handle_info({kind, socket}, %__MODULE__{socket: socket} = state)
      when kind in [:tcp_closed, :ssl_closed] do
    {:disconnect, {:error, :closed}, state}
  end

  def handle_info({kind, socket, data}, %__MODULE__{socket: socket} = state)
      when kind in [:tcp, :ssl] do
    state = %{state | buffer: state.buffer <> data}
    {:noreply, report_event(state)}
  end

  def disconnect({:error, _reason}, %__MODULE__{} = state) do
    state.transport.close(state.socket)
    {:connect, :reconnect, %{state | socket: nil, buffer: <<>>}}
  end

  defp report_active(%{new: false, cluster: cluster, peername: peername} = state, _local_node) do
    Xandra.Cluster.update(cluster, {:control_connection_established, peername})
    {:ok, state}
  end

  defp report_active(
         %{new: true, cluster: cluster, node_ref: node_ref, socket: socket} = state,
         local_node
       ) do
    with {:ok, {address, port}} <- inet_mod(state.transport).peername(socket) do
      Xandra.Cluster.activate(cluster, node_ref, address, port, local_node)
      {:ok, %{state | new: false, peername: address}}
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

  defp maybe_discover_peers(
         _autodiscovery? = false,
         _local_node,
         _transport,
         _socket,
         _protocol_module
       ) do
    {:ok, _peers = nil}
  end

  defp maybe_discover_peers(
         _autodiscovery? = true,
         local_node,
         transport,
         socket,
         protocol_module
       ) do
    # Discover the peers in the same data center as the node we're connected to.
    with {:ok, peers} <- discover_peers(transport, socket, protocol_module) do
      peers = Enum.filter(peers, &(&1.data_center == local_node.data_center))

      {:ok, Enum.map(peers, & &1.address)}
    end
  end

  defp fetch_node_local_info(transport, socket, protocol_module) do
    query = %Simple{
      statement: "SELECT data_center, rack, rpc_address FROM system.local",
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
      {:ok, Cluster.Node.new(local_info)}
    end
  end

  defp discover_peers(transport, socket, protocol_module) do
    query = %Simple{
      statement: "SELECT rpc_address, rack, data_center FROM system.peers",
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
      peers = Enum.map(page, &Cluster.Node.new/1)
      {:ok, peers}
    end
  end

  defp report_event(%{cluster: cluster, buffer: buffer} = state) do
    case decode_frame(buffer, state.protocol_module) do
      {frame, rest} ->
        change_event = state.protocol_module.decode_response(frame)
        Logger.debug("Received event: #{inspect(change_event)}")
        Xandra.Cluster.update(cluster, maybe_enrich_event(state, change_event))
        report_event(%{state | buffer: rest})

      :error ->
        state
    end
  end

  defp maybe_enrich_event(
         state,
         %Cluster.TopologyChange{effect: "NEW_NODE", address: address} = event
       ) do
    case discover_peers(state.transport, state.socket, state.protocol_module) do
      {:ok, peers} ->
        %{event | node: Enum.find(peers, &(&1.address == address))}

      {:error, _} ->
        event
    end
  end

  defp maybe_enrich_event(_state, event), do: event

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
