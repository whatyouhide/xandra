defmodule Xandra.Connection.Utils do
  @moduledoc false

  alias Xandra.{ConnectionError, Error, Frame, Transport}

  @spec recv_frame(Transport.t(), :v4_or_less | :v5_or_more, module | nil) ::
          {:ok, Frame.t(), rest :: binary()}
          | {:error, :closed | :inet.posix()}
  def recv_frame(%Transport{} = transport, protocol_format, compressor)
      when protocol_format in [:v4_or_less, :v5_or_more] and is_atom(compressor) do
    fetch_bytes_fun = fn :no_fetch_state = fetch_state, byte_count ->
      with {:ok, binary} <- Transport.recv(transport, byte_count, 10_000),
           do: {:ok, binary, fetch_state}
    end

    decode_fun =
      case protocol_format do
        :v4_or_less -> &Frame.decode_v4/3
        :v5_or_more -> &Frame.decode_v5/3
      end

    case decode_fun.(fetch_bytes_fun, :no_fetch_state, compressor) do
      {:ok, [frame], rest} -> {:ok, frame, rest}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec request_options(Transport.t(), nil | Frame.supported_protocol(), nil | module) ::
          {:ok, map(), negotiated_protocol_module :: module()}
          | {:error, ConnectionError.t() | Error.t()}
          | {:error,
             {:use_this_protocol_instead, Frame.supported_protocol(), Frame.supported_protocol()}}
          | {:error, {:unsupported_protocol, atom()}}
  def request_options(%Transport{} = transport, protocol_version, compressor \\ nil)
      when (is_nil(protocol_version) or protocol_version in unquote(Frame.supported_protocols())) and
             is_atom(compressor) do
    tentative_protocol_version = protocol_version || Frame.max_supported_protocol()
    tentative_protocol_module = Frame.protocol_version_to_module(tentative_protocol_version)

    payload =
      Frame.new(:options, _options = [])
      |> tentative_protocol_module.encode_request(nil)
      |> Frame.encode_v4(tentative_protocol_module)

    with :ok <- Transport.send(transport, payload),
         {:ok, %Frame{} = frame, rest} <- recv_frame(transport, :v4_or_less, compressor) do
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

  @spec startup_connection(Transport.t(), map(), module(), nil | module(), keyword()) ::
          :ok | {:error, ConnectionError.t()}
  def startup_connection(
        %Transport{} = transport,
        requested_options,
        protocol_module,
        compressor,
        options
      )
      when is_map(requested_options) and is_atom(protocol_module) and is_atom(compressor) do
    # We have to encode the STARTUP frame without compression as in this frame
    # we tell the server which compression algorithm we want to use.
    payload =
      Frame.new(:startup, _options = [])
      |> protocol_module.encode_request(requested_options)
      |> Frame.encode_v4(protocol_module)

    # However, we need to pass the compressor module around when we
    # receive the response to this frame because if we said we want to use
    # compression, this response is already compressed.
    with :ok <- Transport.send(transport, payload),
         tmtry_meas = %{protocol_module: protocol_module, requested_options: requested_options},
         :telemetry.execute([:xandra, :debug, :sent_frame], tmtry_meas, %{frame_type: :STARTUP}),
         {:ok, frame, rest} <- recv_frame(transport, :v4_or_less, compressor) do
      "" = rest
      # TODO: handle :error frames for things like :protocol_violation.
      case frame do
        %Frame{kind: :ready, body: <<>>} ->
          :telemetry.execute([:xandra, :debug, :received_frame], %{}, %{frame_type: :READY})
          :ok

        %Frame{kind: :authenticate} ->
          :telemetry.execute([:xandra, :debug, :received_frame], %{}, %{frame_type: :AUTHENTICATE})

          authenticate_connection(
            transport,
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
         %Transport{} = transport,
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

    with :ok <- Transport.send(transport, payload),
         {:ok, frame, rest} <- recv_frame(transport, protocol_format, compressor) do
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
