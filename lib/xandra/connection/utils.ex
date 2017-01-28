defmodule Xandra.Connection.Utils do
  @moduledoc false

  alias Xandra.{Connection.Error, Frame, Protocol}

  @spec recv_frame(:gen_tcp.socket, nil | module) ::
        {:ok, Frame.t} | {:error, :closed | :inet.posix}
  def recv_frame(socket, compressor \\ nil) when is_atom(compressor) do
    length = Frame.header_length()

    with {:ok, header} <- :gen_tcp.recv(socket, length) do
      case Frame.body_length(header) do
        0 ->
          {:ok, Frame.decode(header)}
        body_length ->
          with {:ok, body} <- :gen_tcp.recv(socket, body_length),
               do: {:ok, Frame.decode(header, body, compressor)}
      end
    end
  end

  @spec request_options(:gen_tcp.socket) :: {:ok, term} | {:error, Error.t}
  def request_options(socket) do
    payload =
      Frame.new(:options)
      |> Protocol.encode_request(nil)
      |> Frame.encode()

    with :ok <- :gen_tcp.send(socket, payload),
         {:ok, %Frame{} = frame} <- recv_frame(socket) do
      {:ok, Protocol.decode_response(frame)}
    else
      {:error, reason} ->
        {:error, Error.new("request options", reason)}
    end
  end

  @spec startup_connection(:gen_tcp.socket, map, nil | module) :: :ok | {:error, Error.t}
  def startup_connection(socket, requested_options, compressor \\ nil)
      when is_map(requested_options) and is_atom(compressor) do
    # We have to encode the STARTUP frame without compression as in this frame
    # we tell the server which compression algorithm we want to use.
    payload =
      Frame.new(:startup)
      |> Protocol.encode_request(requested_options)
      |> Frame.encode()

    # However, we need to pass the compressor module around when we
    # receive the response to this frame because if we said we want to use
    # compression, this response is already compressed.
    with :ok <- :gen_tcp.send(socket, payload),
         {:ok, %Frame{body: <<>>}} <- recv_frame(socket, compressor) do
       :ok
    else
      {:error, reason} ->
        {:error, Error.new("startup connection", reason)}
    end
  end
end
