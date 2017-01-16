defmodule Xandra.Connection do
  @moduledoc false

  use DBConnection

  alias Xandra.{Prepared, Frame, Protocol}
  alias __MODULE__.{Error, Utils}

  @default_timeout 5_000
  @default_socket_options [packet: :raw, mode: :binary, active: false]

  defstruct [:socket, :prepared_cache]

  def connect(options) do
    host = Keyword.fetch!(options, :host)
    port = Keyword.fetch!(options, :port)
    prepared_cache = Keyword.fetch!(options, :prepared_cache)

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

  def handle_prepare(%Prepared{} = prepared, options, %__MODULE__{socket: socket} = state) do
    force? = Keyword.get(options, :force, false)
    case prepared_cache_lookup(state, prepared, force?) do
      {:ok, prepared} ->
        {:ok, prepared, state}
      :error ->
        payload =
          Frame.new(:prepare)
          |> Protocol.encode_request(prepared)
          |> Frame.encode()

        with :ok <- :gen_tcp.send(socket, payload),
             {:ok, %Frame{} = frame} <- Utils.recv_frame(socket),
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

  def handle_execute(_query, payload, _options, %__MODULE__{socket: socket} = state) do
    with :ok <- :gen_tcp.send(socket, payload),
        {:ok, %Frame{} = frame} <- Utils.recv_frame(socket) do
      {:ok, frame, state}
    else
      {:error, reason} ->
        {:disconnect, Error.new("execute", reason), state}
    end
  end

  def handle_close(query, _options, state) do
    {:ok, query, state}
  end

  def disconnect(_exception, %__MODULE__{socket: socket}) do
    :ok = :gen_tcp.close(socket)
  end

  def ping(%__MODULE__{socket: socket} = state) do
    case Utils.request_options(socket) do
      {:ok, _options} ->
        {:ok, state}
      {:error, reason} ->
        {:disconnect, reason, state}
    end
  end

  defp prepared_cache_lookup(state, prepared, true) do
    Prepared.Cache.delete(state.prepared_cache, prepared)
    :error
  end

  defp prepared_cache_lookup(state, prepared, false) do
    Prepared.Cache.lookup(state.prepared_cache, prepared)
  end

  defp connect(host, port) do
    with {:error, reason} <- :gen_tcp.connect(host, port, @default_socket_options, @default_timeout),
         do: {:error, Error.new("TCP connect", reason)}
  end
end
