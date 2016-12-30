defmodule Xandra.Connection do
  use DBConnection

  alias Xandra.{Connection.Utils, Query, Frame, Protocol}

  defmodule Error do
    defexception [:action, :reason]

    def message(%__MODULE__{} = exception) do
      inspect(exception)
    end

    def exception(args) do
      Kernel.struct!(%__MODULE__{}, args)
    end
  end

  @default_timeout 5_000
  @default_sock_opts [packet: :raw, mode: :binary, active: false]

  def connect(opts) do
    host = Keyword.fetch!(opts, :host) |> to_char_list()
    port = Keyword.get(opts, :port, 9042)
    case :gen_tcp.connect(host, port, @default_sock_opts, @default_timeout) do
      {:ok, sock} ->
        {:ok, options} = Utils.request_options(sock)
        :ok = Utils.startup_connection(sock, options)
        {:ok, %{sock: sock}}
      {:error, reason} ->
        {:error, Error.exception(action: "connect", reason: reason)}
    end
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

    case :gen_tcp.send(sock, payload) do
      :ok ->
        {:ok, %Frame{} = frame} = Utils.recv_frame(sock)
        {:ok, Protocol.decode_response(frame, query), state}
      {:error, reason} ->
        {:disconnect, reason, state}
    end
  end

  def handle_execute(_query, payload, _opts, %{sock: sock} = state) do
    with :ok <- :gen_tcp.send(sock, payload),
        {:ok, %Frame{} = frame} <- Utils.recv_frame(sock) do
      {:ok, frame, state}
    else
      {:error, reason} ->
        {:disconnect, reason, state}
    end
  end

  def handle_close(query, _opts, state) do
    {:ok, query, state}
  end

  def disconnect(_exception, %{sock: sock}) do
    :ok = :gen_tcp.close(sock)
  end
end
