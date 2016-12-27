defmodule Xandra.Connection do
  use DBConnection

  alias Xandra.{Query, Frame, Protocol}

  @default_timeout 5_000
  @default_sock_opts [packet: :raw, mode: :binary, active: false]

  def start_link() do
    DBConnection.start_link(__MODULE__, [host: "127.0.0.1"])
  end

  def connect(opts) do
    host = Keyword.fetch!(opts, :host) |> to_char_list()
    port = Keyword.get(opts, :port, 9042)
    case :gen_tcp.connect(host, port, @default_sock_opts, @default_timeout) do
      {:ok, sock} ->
        options = request_options(sock)
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

  def execute(conn, statement, params, opts) do
    with {:ok, query} <- Query.new(statement) do
      DBConnection.execute(conn, query, params, opts)
    end
  end

  def handle_execute(query, _params, _opts, %{sock: sock} = state) do
    body =
      <<byte_size(query.statement)::32>> <>
      query.statement <>
      encode_consistency_level(:one) <>
      encode_query_flags()
    payload = %Frame{opcode: 0x07} |> Frame.encode(body)
    case :gen_tcp.send(sock, payload) do
      :ok ->
        {:ok, recv(sock), state}
      {:error, reason} ->
        {:disconnect, reason, state}
    end
  end

  @consistency_levels %{
    0x0000 => :any,
    0x0001 => :one,
    0x0002 => :two,
    0x0003 => :three,
    0x0004 => :quorum,
    0x0005 => :all,
    0x0006 => :local_quorum,
    0x0007 => :each_quorum,
    0x0008 => :serial,
    0x0009 => :local_serial,
    0x000A => :local_one,
  }

  for {spec, level} <- @consistency_levels do
    defp encode_consistency_level(unquote(level)) do
      <<unquote(spec)::16>>
    end
  end

  defp encode_query_flags() do
    <<0x00>>
  end

  defp startup_connection(sock, %{"CQL_VERSION" => [cql_version | _]}) do
    body = encode_string_map(%{"CQL_VERSION" => cql_version})
    payload = %Frame{opcode: 0x01} |> Frame.encode(body)
    case :gen_tcp.send(sock, payload) do
      :ok ->
        recv(sock)
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
        recv(sock)
      {:error, reason} ->
        reason
    end
  end

  defp recv(sock) do
    case :gen_tcp.recv(sock, 9) do
      {:ok, <<header::5-bytes, 0::32>>} ->
        Protocol.decode_response(header, "")

      {:ok, <<header::5-bytes, length::32>>} ->
        case :gen_tcp.recv(sock, length) do
          {:ok, body} ->
            Protocol.decode_response(header, body)
          {:error, _reason} = error ->
            error
        end
      {:error, _reason} = error ->
        error
    end
  end
end
