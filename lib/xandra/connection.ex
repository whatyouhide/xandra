defmodule Xandra.Connection do
  use DBConnection

  alias Xandra.{Query, Frame, Protocol}

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
        {:ok, options} = request_options(sock)
        startup_connection(sock, options)
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

  def handle_prepare(%Query{statement: statement} = query, _opts, %{sock: sock} = state) do
    body = <<byte_size(statement)::32>> <> statement
    payload = Frame.new(:prepare, body) |> Frame.encode()
    case :gen_tcp.send(sock, payload) do
      :ok ->
        {:ok, frame} = recv(sock)
        {:ok, Protocol.decode_response(frame, query), state}
      {:error, reason} ->
        {:disconnect, reason, state}
    end
  end

  def handle_execute(_query, payload, _opts, %{sock: sock} = state) do
    with :ok <- :gen_tcp.send(sock, payload),
        {:ok, frame} <- recv(sock) do
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

  defp startup_connection(sock, %{"CQL_VERSION" => [cql_version | _]}) do
    body = encode_string_map(%{"CQL_VERSION" => cql_version})
    payload = Frame.new(:startup, body) |> Frame.encode()
    case :gen_tcp.send(sock, payload) do
      :ok ->
        {:ok, %{body: <<>>}} = recv(sock)
        :ok
      {:error, reason} ->
        reason
    end
  end

  defp encode_string_map(map) do
    for {key, value} <- map, into: <<map_size(map)::16>> do
      key_size = byte_size(key)
      <<key_size::16, key::size(key_size)-bytes, byte_size(value)::16, value::bytes>>
    end
  end

  defp request_options(sock) do
    payload = Frame.new(:options) |> Frame.encode()
    case :gen_tcp.send(sock, payload) do
      :ok ->
        with {:ok, frame} <- recv(sock) do
          {:ok, Protocol.decode_response(frame)}
        end
      {:error, reason} ->
        {:error, Error.exception(action: "request_options", reason: reason)}
    end
  end

  defp recv(sock) do
    length = Frame.header_length()
    with {:ok, header} <- :gen_tcp.recv(sock, length) do
      case Frame.body_length(header) do
        0 ->
          {:ok, Frame.decode(header)}
        length ->
          with {:ok, body} <- :gen_tcp.recv(sock, length) do
            {:ok, Frame.decode(header, body)}
          end
      end
    end
  end
end
