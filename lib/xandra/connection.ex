defmodule Xandra.Connection do
  use DBConnection

  alias Xandra.{Connection.Error, Connection.Utils, Query, Frame, Protocol}

  @default_timeout 5_000
  @default_sock_opts [packet: :raw, mode: :binary, active: false]

  def connect(opts) do
    host = Keyword.fetch!(opts, :host) |> to_char_list()
    port = Keyword.get(opts, :port, 9042)

    with {:ok, sock} <- connect(host, port),
         {:ok, options} <- Utils.request_options(sock),
         :ok <- Utils.startup_connection(sock, options),
         do: {:ok, %{sock: sock}}
  end

  def checkout(state) do
    {:ok, state}
  end

  def checkin(state) do
    {:ok, state}
  end

  def handle_prepare(%Query{} = query, _opts, %{sock: sock} = state) do
    payload =
      Frame.new(:prepare)
      |> Protocol.encode_request(query)
      |> Frame.encode()

    with :ok <- :gen_tcp.send(sock, payload),
         {:ok, %Frame{} = frame} = Utils.recv_frame(sock) do
      {:ok, Protocol.decode_response(frame, query), state}
    else
      {:error, reason} ->
        {:disconnect, Error.new("prepare", reason), state}
    end
  end

  def handle_execute(_query, payload, _opts, %{sock: sock} = state) do
    with :ok <- :gen_tcp.send(sock, payload),
        {:ok, %Frame{} = frame} <- Utils.recv_frame(sock) do
      {:ok, frame, state}
    else
      {:error, reason} ->
        {:disconnect, Error.new("execute", reason), state}
    end
  end

  def handle_close(query, _opts, state) do
    {:ok, query, state}
  end

  def disconnect(_exception, %{sock: sock}) do
    :ok = :gen_tcp.close(sock)
  end

  defp connect(host, port) do
    with {:error, reason} <- :gen_tcp.connect(host, port, @default_sock_opts, @default_timeout),
         do: {:error, Error.new("TCP connect", reason)}
  end
end
