defmodule Xandra.Cluster.ControlConnection do
  use Connection

  alias Xandra.{
    Frame,
    Protocol,
    Connection.Utils
  }

  alias Xandra.Connection.Transport

  require Logger

  @default_backoff 5_000
  @default_timeout 5_000
  @socket_options [packet: :raw, mode: :binary, active: false]

  defstruct [
    :cluster,
    :node_ref,
    :address,
    :port,
    :transport,
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
    case Transport.connect(address, port, @socket_options, @default_timeout) do
      {:ok, transport} ->
        state = %{state | transport: transport}

        with {:ok, transport} <- upgrade_protocol(transport, options),
             {:ok, supported_options} <- Utils.request_options(transport),
             :ok <- startup_connection(transport, supported_options, options),
             :ok <- register_to_events(transport),
             :ok <- Transport.setopts(transport, active: true),
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

  def handle_info(message, %__MODULE__{transport: {:gen_tcp, socket}} = state) do
    case message do
      {:tcp_error, ^socket, reason} ->
        {:disconnect, {:error, reason}, state}

      {:tcp_closed, ^socket} ->
        {:disconnect, {:error, :closed}, state}

      {:tcp, ^socket, data} ->
        state = %{state | buffer: state.buffer <> data}
        {:noreply, report_event(state)}
    end
  end

  def disconnect({:error, _reason}, %__MODULE__{transport: transport} = state) do
    Transport.close(transport)
    # NOTE: really nil here???
    {:connect, :reconnect, %{state | transport: nil, buffer: <<>>}}
  end

  defp upgrade_protocol(transport, options) do
    encryption_options = Keyword.get(options, :encryption, false)

    case Transport.maybe_upgrade_protocol(transport, encryption_options) do
      {:ok, handler} ->
        {:ok, handler}

      {:error, reason} ->
        {:error, Xandra.ConnectionError.new("upgrade protocol", reason)}
    end
  end

  defp report_active(%{new: false} = state) do
    {:ok, state}
  end

  defp report_active(
         %{new: true, cluster: cluster, node_ref: node_ref, transport: transport} = state
       ) do
    with {:ok, {address, port}} <- Transport.peername(transport) do
      Xandra.Cluster.activate(cluster, node_ref, address, port)
      {:ok, %{state | new: false, address: address}}
    end
  end

  defp startup_connection(transport, supported_options, options) do
    %{"CQL_VERSION" => [cql_version | _]} = supported_options
    requested_options = %{"CQL_VERSION" => cql_version}
    Utils.startup_connection(transport, requested_options, nil, options)
  end

  defp register_to_events(transport) do
    payload =
      Frame.new(:register)
      |> Protocol.encode_request(["STATUS_CHANGE"])
      |> Frame.encode()

    with :ok <- Transport.send(transport, payload),
         {:ok, %Frame{} = frame} <- Utils.recv_frame(transport) do
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
