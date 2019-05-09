defmodule Xandra.Cluster.ControlConnection do
  @moduledoc false

  use Connection

  alias Xandra.{Frame, Protocol, Connection.Utils}

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
    new: true,
    buffer: <<>>
  ]

  def start_link(cluster, node_ref, address, port, options) do
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
      transport: if(options[:ssl], do: :ssl, else: :gen_tcp),
      transport_options: transport_options
    }

    Connection.start_link(__MODULE__, state)
  end

  def init(state) do
    {:connect, :init, state}
  end

  def connect(_action, %__MODULE__{address: address, port: port, options: options} = state) do
    case state.transport.connect(address, port, state.transport_options, @default_timeout) do
      {:ok, socket} ->
        state = %{state | socket: socket}

        with {:ok, supported_options} <- Utils.request_options(state.transport, socket),
             :ok <- startup_connection(state.transport, socket, supported_options, options),
             :ok <- register_to_events(state.transport, socket),
             :ok <- inet_mod(state.transport).setopts(socket, active: true),
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

  defp report_active(%{new: false} = state) do
    {:ok, state}
  end

  defp report_active(%{new: true, cluster: cluster, node_ref: node_ref, socket: socket} = state) do
    with {:ok, {address, port}} <- inet_mod(state.transport).peername(socket) do
      Xandra.Cluster.activate(cluster, node_ref, address, port)
      {:ok, %{state | new: false, address: address}}
    end
  end

  defp startup_connection(transport, socket, supported_options, options) do
    %{"CQL_VERSION" => [cql_version | _]} = supported_options
    requested_options = %{"CQL_VERSION" => cql_version}
    Utils.startup_connection(transport, socket, requested_options, nil, options)
  end

  defp register_to_events(transport, socket) do
    payload =
      Frame.new(:register)
      |> Protocol.encode_request(["STATUS_CHANGE"])
      |> Frame.encode()

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <- Utils.recv_frame(transport, socket) do
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

  defp inet_mod(:gen_tcp), do: :inet
  defp inet_mod(:ssl), do: :ssl
end
