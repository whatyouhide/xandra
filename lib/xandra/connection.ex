defmodule Xandra.Connection do
  use DBConnection

  alias Xandra.{Prepared, Frame, Protocol}
  alias __MODULE__.{Error, Utils}

  @default_timeout 5_000
  @default_socket_opts [packet: :raw, mode: :binary, active: false]

  defstruct [:socket, :prepared_cache]

  def connect(opts) do
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    prepared_cache = Keyword.fetch!(opts, :prepared_cache)

    with {:ok, socket} <- connect(host, port),
         {:ok, options} <- Utils.request_options(socket),
         :ok <- Utils.startup_connection(socket, options),
         do: {:ok, %__MODULE__{socket: socket, prepared_cache: prepared_cache}}
  end

  def checkout(state) do
    {:ok, state}
  end

  def checkin(state) do
    {:ok, state}
  end

  def handle_prepare(%Prepared{} = prepared, opts, %__MODULE__{socket: socket} = state) do
    force? = Keyword.get(opts, :force, false)
    case prepared_cache_lookup(state, prepared, force?) do
      {:ok, prepared} ->
        {:ok, prepared, state}
      :error ->
        payload =
          Frame.new(:prepare)
          |> Protocol.encode_request(prepared)
          |> Frame.encode()

        with :ok <- :gen_tcp.send(socket, payload),
             {:ok, %Frame{} = frame} = Utils.recv_frame(socket),
             %Prepared{} = prepared <- Protocol.decode_response(frame, prepared) do
          Prepared.Cache.insert(state.prepared_cache, prepared)
          {:ok, prepared, state}
        else
          {:error, reason} ->
            {:disconnect, Error.new("prepare", reason), state}
          %Xandra.Error{} = reason ->
            {:error, reason, state}
        end
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

  def prepared_cache_lookup(state, prepared, true) do
    Prepared.Cache.delete(state.prepared_cache, prepared)
    :error
  end

  def prepared_cache_lookup(state, prepared, false) do
    Prepared.Cache.lookup(state.prepared_cache, prepared)
  end

  defp connect(host, port) do
    with {:error, reason} <- :gen_tcp.connect(host, port, @default_socket_opts, @default_timeout),
         do: {:error, Error.new("TCP connect", reason)}
  end
end
