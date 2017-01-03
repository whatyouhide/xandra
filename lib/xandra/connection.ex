defmodule Xandra.Connection do
  use DBConnection

  alias Xandra.{Prepared, Frame, Protocol}
  alias __MODULE__.{Error, Utils}

  @default_timeout 5_000
  @default_socket_opts [packet: :raw, mode: :binary, active: false]

  defstruct [:socket]

  def connect(opts) do
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)

    with {:ok, socket} <- connect(host, port),
         {:ok, options} <- Utils.request_options(socket),
         :ok <- Utils.startup_connection(socket, options),
         do: {:ok, %__MODULE__{socket: socket}}
  end

  def checkout(state) do
    {:ok, state}
  end

  def checkin(state) do
    {:ok, state}
  end

  def handle_prepare(%Prepared{} = prepared, _opts, %__MODULE__{socket: socket} = state) do
    payload =
      Frame.new(:prepare)
      |> Protocol.encode_request(prepared)
      |> Frame.encode()

    with :ok <- :gen_tcp.send(socket, payload),
         {:ok, %Frame{} = frame} = Utils.recv_frame(socket) do
      {:ok, Protocol.decode_response(frame, prepared), state}
    else
      {:error, reason} ->
        {:disconnect, Error.new("prepare", reason), state}
    end
  end

  def handle_execute(_query, payload, _opts, %__MODULE__{socket: socket} = state) do
    with :ok <- :gen_tcp.send(socket, payload),
        {:ok, %Frame{} = frame} <- Utils.recv_frame(socket) do
      {:ok, frame, state}
    else
      {:error, reason} ->
        {:disconnect, Error.new("execute", reason), state}
    end
  end

  def handle_close(query, _opts, state) do
    {:ok, query, state}
  end

  def disconnect(_exception, %__MODULE__{socket: socket}) do
    :ok = :gen_tcp.close(socket)
  end

  defp connect(host, port) do
    with {:error, reason} <- :gen_tcp.connect(host, port, @default_socket_opts, @default_timeout),
         do: {:error, Error.new("TCP connect", reason)}
  end
end
