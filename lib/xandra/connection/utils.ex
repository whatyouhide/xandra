defmodule Xandra.Connection.Utils do
  @moduledoc false

  alias Xandra.{ConnectionError, Error, Frame, Protocol.CRC}

  require Logger

  @typep transport :: :gen_tcp | :ssl
  @typep socket :: :gen_tcp.socket() | :ssl.sslsocket()

  @spec recv_frame(transport, socket, protocol_format :: :v4_or_less | :v5_or_more, module | nil) ::
          {:ok, Frame.t()} | {:error, :closed | :inet.posix()}
  def recv_frame(transport, socket, protocol_format, compressor)
      when transport in [:gen_tcp, :ssl] and protocol_format in [:v4_or_less, :v5_or_more] and
             is_atom(compressor) do
    case protocol_format do
      :v4_or_less -> recv_frame_v4(transport, socket, compressor)
      :v5_or_more -> recv_frame_v5(transport, socket, compressor)
    end
  end

  defp recv_frame_v4(transport, socket, compressor) do
    length = Frame.header_length()

    with {:ok, header} <- transport.recv(socket, length) do
      case Frame.body_length(header) do
        0 ->
          {:ok, Frame.decode(header)}

        body_length ->
          with {:ok, body} <- transport.recv(socket, body_length),
               do: {:ok, Frame.decode(header, body, compressor)}
      end
    end
  end

  defp recv_frame_v5(transport, socket, compressor) do
    recv_frame_v5(transport, socket, compressor, _payload_acc = <<>>)
  end

  defp recv_frame_v5(transport, socket, compressor, payload_acc) do
    with {:ok, header} <- transport.recv(socket, 6) do
      <<header_contents::3-bytes, crc24_of_header::24-integer-little>> = header

      if crc24_of_header != CRC.crc24(header_contents) do
        raise "mismatching CRC for header"
      end

      <<payload_length::17-integer-little, is_self_contained_flag::1, _header_padding::6>> =
        header_contents

      self_contained? = is_self_contained_flag == 0

      case transport.recv(socket, payload_length + 4) do
        {:ok, <<payload::size(payload_length)-bytes, crc32_of_payload::32-integer-little>>} ->
          if crc32_of_payload != CRC.crc32(payload) do
            raise "mismatching CRC32 for payload"
          end

          if self_contained? do
            {:ok, Frame.decode_from_binary(payload, compressor)}
          else
            recv_frame_v5(transport, socket, compressor, payload_acc <> payload)
          end
      end
    end
  end

  @spec request_options(:gen_tcp | :ssl, term, nil | Frame.supported_protocol(), nil | module) ::
          {:ok, map(), negotiated_protocol_module :: module()}
          | {:error, ConnectionError.t() | Error.t(),
             {:use_this_protocol_instead, Frame.supported_protocol()}}
  def request_options(transport, socket, protocol_version, compressor \\ nil)
      when transport in [:gen_tcp, :ssl] and
             (is_nil(protocol_version) or protocol_version in unquote(Frame.supported_protocols())) and
             is_atom(compressor) do
    tentative_protocol_module =
      Frame.protocol_version_to_module(protocol_version || Frame.max_supported_protocol())

    payload =
      Frame.new(:options)
      |> tentative_protocol_module.encode_request(nil)
      |> Frame.encode_v4(tentative_protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <- recv_frame(transport, socket, :v4_or_less, compressor) do
      case tentative_protocol_module.decode_response(frame) do
        %Error{} = error ->
          if error.message =~ "unsupported protocol version" do
            if frame.protocol_version in Frame.supported_protocols() do
              {:error, {:use_this_protocol_instead, frame.protocol_version}}
            else
              {:error, {:unsupported_protocol, frame.protocol_version}}
            end
          else
            {:error, error}
          end

        %{} = options ->
          {:ok, options, tentative_protocol_module}
      end
    else
      {:error, reason} ->
        {:error, ConnectionError.new("request options", reason)}
    end
  end

  @spec startup_connection(:gen_tcp | :ssl, term, map, module, nil | module) ::
          :ok | {:error, ConnectionError.t()}
  def startup_connection(
        transport,
        socket,
        requested_options,
        protocol_module,
        compressor \\ nil,
        options \\ []
      )
      when is_map(requested_options) and is_atom(protocol_module) and is_atom(compressor) do
    # We have to encode the STARTUP frame without compression as in this frame
    # we tell the server which compression algorithm we want to use.
    payload =
      Frame.new(:startup)
      |> protocol_module.encode_request(requested_options)
      |> Frame.encode_v4(protocol_module)

    # However, we need to pass the compressor module around when we
    # receive the response to this frame because if we said we want to use
    # compression, this response is already compressed.
    with :ok <- transport.send(socket, payload),
         {:ok, frame} <- recv_frame(transport, socket, :v4_or_less, compressor) do
      # TODO: handle :error frames for things like :protocol_violation.
      case frame do
        %Frame{kind: :ready, body: <<>>} ->
          Logger.debug("Received READY frame")
          :ok

        %Frame{kind: :authenticate} ->
          Logger.debug("Received AUTHENTICATE frame, authenticating connection")

          authenticate_connection(
            transport,
            socket,
            requested_options,
            protocol_module,
            compressor,
            options
          )

        _ ->
          raise "protocol violation, got unexpected frame: #{inspect(frame)}"
      end
    else
      {:error, reason} ->
        {:error, ConnectionError.new("startup connection", reason)}
    end
  end

  defp authenticate_connection(
         transport,
         socket,
         requested_options,
         protocol_module,
         compressor,
         options
       ) do
    payload =
      Frame.new(:auth_response)
      |> protocol_module.encode_request(requested_options, options)
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, frame} <- recv_frame(transport, socket, protocol_format, compressor) do
      case frame do
        %Frame{kind: :auth_success} -> :ok
        %Frame{kind: :error} -> {:error, protocol_module.decode_response(frame)}
        _ -> raise "protocol violation, got unexpected frame: #{inspect(frame)}"
      end
    else
      {:error, reason} ->
        {:error, ConnectionError.new("authenticate connection", reason)}
    end
  end
end
