defmodule Xandra.Cluster.ControlConnection do
  use Connection

  alias Xandra.{Frame, Protocol, Connection.Utils}

  require Logger

  @default_backoff 5_000
  @default_timeout 5_000
  @socket_options [packet: :raw, mode: :binary, active: false]

  defstruct [
    :cluster,
    :node_ref,
    :address,
    :port,
    :socket,
    :options,
    new: true,
    buffer: <<>>
  ]

  def start_link(cluster, node_ref, address, port, options) do
    state = %__MODULE__{
      cluster: cluster,
      node_ref: node_ref,
      address: address,
      port: port,
      options: options
    }

    Connection.start_link(__MODULE__, state)
  end

  def init(state) do
    {:connect, :init, state}
  end

  def connect(_action, %__MODULE__{address: address, port: port, options: options} = state) do
    case :gen_tcp.connect(address, port, @socket_options, @default_timeout) do
      {:ok, socket} ->
        state = %{state | socket: socket}

        with {:ok, supported_options} <- Utils.request_options(socket),
             :ok <- startup_connection(socket, supported_options, options),
             :ok <- register_to_events(socket),
             :ok <- :inet.setopts(socket, active: true),
             {:ok, state} <- report_active(state) do
          {:ok, state}
        else
          {:error, _reason} = error ->
            {:connect, :reconnect, state} = disconnect(error, state)
            {:backoff, @default_backoff, state}
        end

      {:error, _reason} ->
        {:backoff, @default_backoff, state}
    end
  end

  def handle_info({:tcp_error, socket, reason}, %__MODULE__{socket: socket} = state) do
    {:disconnect, {:error, reason}, state}
  end

  def handle_info({:tcp_closed, socket}, %__MODULE__{socket: socket} = state) do
    {:disconnect, {:error, :closed}, state}
  end

  def handle_info({:tcp, socket, data}, %__MODULE__{socket: socket} = state) do
    state = %{state | buffer: state.buffer <> data}
    {:noreply, report_event(state)}
  end

  def disconnect({:error, _reason}, %__MODULE__{} = state) do
    :gen_tcp.close(state.socket)
    {:connect, :reconnect, %{state | socket: nil, buffer: <<>>}}
  end

  defp report_active(%{new: false} = state) do
    {:ok, state}
  end

  defp report_active(%{new: true, cluster: cluster, node_ref: node_ref, socket: socket} = state) do
    with {:ok, {address, port}} <- :inet.peername(socket) do
      Xandra.Cluster.activate(cluster, node_ref, address, port)
      {:ok, %{state | new: false, address: address}}
    end
  end

  defp startup_connection(socket, supported_options, options) do
    %{"CQL_VERSION" => [cql_version | _]} = supported_options
    requested_options = %{"CQL_VERSION" => cql_version}
    Utils.startup_connection(socket, requested_options, nil, options)
  end

  defp register_to_events(socket) do
    payload =
      Frame.new(:register)
      |> Protocol.encode_request(["STATUS_CHANGE"])
      |> Frame.encode()

    with :ok <- :gen_tcp.send(socket, payload),
         {:ok, %Frame{} = frame} <- Utils.recv_frame(socket) do
      :ok = Protocol.decode_response(frame)
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp report_event(%{cluster: cluster, buffer: buffer} = state) do
    case decode_frame(buffer) do
      {frame, rest} ->
        status_change = Protocol.decode_response(frame)
        Logger.debug("Received STATUS_CHANGE event: #{inspect(status_change)}")
        Xandra.Cluster.update(cluster, status_change)
        report_event(%{state | buffer: rest})

      :error ->
        state
    end
  end

  defp decode_frame(buffer) do
    header_length = Frame.header_length()

    case buffer do
      <<header::size(header_length)-bytes, rest::binary>> ->
        body_length = Frame.body_length(header)

        case rest do
          <<body::size(body_length)-bytes, rest::binary>> ->
            {Frame.decode(header, body), rest}

          _ ->
            :error
        end

      _ ->
        :error
    end
  end
end
