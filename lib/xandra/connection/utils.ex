defmodule Xandra.Connection.Utils do
  @moduledoc false

  alias Xandra.{Connection.Error, Frame, Protocol}

  @spec recv_frame(:gen_tcp.socket) ::
        {:ok, Frame.t} | {:error, :closed | :inet.posix}
  def recv_frame(socket) do
    length = Frame.header_length()

    with {:ok, header} <- :gen_tcp.recv(socket, length) do
      case Frame.body_length(header) do
        0 ->
          {:ok, Frame.decode(header)}
        body_length ->
          with {:ok, body} <- :gen_tcp.recv(socket, body_length),
               do: {:ok, Frame.decode(header, body)}
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

  @spec startup_connection(:gen_tcp.socket, map) :: :ok | {:error, Error.t}
  def startup_connection(socket, requested_options) when is_map(requested_options) do
    payload =
      Frame.new(:startup)
      |> Protocol.encode_request(requested_options)
      |> Frame.encode()

    with :ok <- :gen_tcp.send(socket, payload),
         {:ok, %Frame{body: <<>>}} <- recv_frame(socket) do
       :ok
    else
      {:error, reason} ->
        {:error, Error.new("startup connection", reason)}
    end
  end
end
