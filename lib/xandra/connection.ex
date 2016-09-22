defmodule Xandra.Connection do
  use DBConnection

  alias Xandra.{Query, Frame, Protocol}

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
        {:error, "connect error: " <> inspect(reason)}
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
    payload = %Frame{opcode: 0x09} |> Frame.encode(body)
    case :gen_tcp.send(sock, payload) do
      :ok ->
        {:ok, {header, body}} = recv(sock)
        {:ok, Protocol.decode_response(header, body, query), state}
      {:error, reason} ->
        {:disconnect, reason, state}
    end
  end

  def handle_execute(_query, frame, _opts, %{sock: sock} = state) do
    with :ok <- :gen_tcp.send(sock, frame),
        {:ok, response} <- recv(sock) do
      {:ok, response, state}
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
    payload = %Frame{opcode: 0x01} |> Frame.encode(body)
    case :gen_tcp.send(sock, payload) do
      :ok ->
        {:ok, {_, <<>>}} = recv(sock)
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
    payload = %Frame{opcode: 0x05} |> Frame.encode()
    case :gen_tcp.send(sock, payload) do
      :ok ->
        with {:ok, {header, body}} <- recv(sock) do
          {:ok, Protocol.decode_response(header, body)}
        end
      {:error, reason} ->
        reason
    end
  end

  defp recv(sock) do
    with {:ok, header} <- :gen_tcp.recv(sock, 9) do
      case Frame.body_length(header) do
        0 ->
          {:ok, {header, <<>>}}
        length ->
          with {:ok, body} <- :gen_tcp.recv(sock, length) do
            {:ok, {header, body}}
          end
      end
    end
  end
end
