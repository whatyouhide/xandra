defmodule Xandra.Connection.Utils do
  @moduledoc false

  alias Xandra.{ConnectionError, Error, Frame}

  require Logger

  @typep transport :: :gen_tcp | :ssl
  @typep socket :: :gen_tcp.socket() | :ssl.sslsocket()

  @spec recv_frame(transport, socket, protocol_format :: :v4_or_less | :v5_or_more, module | nil) ::
          {:ok, Frame.t(), rest :: binary()} | {:error, :closed | :inet.posix()}
  def recv_frame(transport, socket, protocol_format, compressor)
      when transport in [:gen_tcp, :ssl] and protocol_format in [:v4_or_less, :v5_or_more] and
             is_atom(compressor) do
    fetch_bytes_fun = fn fetch_state, byte_count ->
      with {:ok, binary} <- transport.recv(socket, byte_count), do: {:ok, binary, fetch_state}
    end

    case protocol_format do
      :v4_or_less -> Frame.decode_v4(fetch_bytes_fun, :no_fetch_state, compressor)
      :v5_or_more -> Frame.decode_v5(fetch_bytes_fun, :no_fetch_state, compressor)
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
    tentative_protocol_version = protocol_version || Frame.max_supported_protocol()
    tentative_protocol_module = Frame.protocol_version_to_module(tentative_protocol_version)

    payload =
      Frame.new(:options, _options = [])
      |> tentative_protocol_module.encode_request(nil)
      |> Frame.encode_v4(tentative_protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame, rest} <- recv_frame(transport, socket, :v4_or_less, compressor) do
      "" = rest

      case tentative_protocol_module.decode_response(frame) do
        # If a protocol version is forced, we should not gracefully downgrade.
        %Error{} = error when not is_nil(protocol_version) ->
          {:error, error}

        %Error{} = error ->
          cond do
            error.message =~ "unsupported protocol version" ->
              if frame.protocol_version in Frame.supported_protocols() do
                {:error,
                 {:use_this_protocol_instead, tentative_protocol_version, frame.protocol_version}}
              else
                {:error, {:unsupported_protocol, frame.protocol_version}}
              end

            error.message =~ "Beta version of the protocol" and
                error.message =~ "but USE_BETA flag is unset" ->
              {:error,
               {:use_this_protocol_instead, tentative_protocol_version,
                Frame.previous_protocol(frame.protocol_version)}}

            true ->
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

  @spec ping(:gen_tcp | :ssl, term, nil | Frame.supported_protocol(), nil | module) ::
          :ok
          | {:error, ConnectionError.t() | Error.t(),
             {:use_this_protocol_instead, Frame.supported_protocol()}}
  def ping(transport, socket, protocol_module, compressor)
      when transport in [:gen_tcp, :ssl] and is_atom(protocol_module) and is_atom(compressor) do
    payload =
      Frame.new(:options, compressor: compressor)
      |> protocol_module.encode_request(nil)
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame, rest} <-
           recv_frame(transport, socket, protocol_format, compressor) do
      "" = rest

      case protocol_module.decode_response(frame) do
        %Error{} = error -> {:error, error}
        %{} = _options -> :ok
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
      Frame.new(:startup, _options = [])
      |> protocol_module.encode_request(requested_options)
      |> Frame.encode_v4(protocol_module)

    Logger.debug(
      "Sending STARTUP frame with protocol #{inspect(protocol_module)} and " <>
        "requested options: #{inspect(requested_options)}"
    )

    # However, we need to pass the compressor module around when we
    # receive the response to this frame because if we said we want to use
    # compression, this response is already compressed.
    with :ok <- transport.send(socket, payload),
         {:ok, frame, rest} <- recv_frame(transport, socket, :v4_or_less, compressor) do
      "" = rest
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
      Frame.new(:auth_response, _options = [])
      |> protocol_module.encode_request(requested_options, options)
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, frame, rest} <- recv_frame(transport, socket, protocol_format, compressor) do
      "" = rest

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
