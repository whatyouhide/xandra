defmodule Xandra.Connection.Utils do
  @moduledoc false

  alias Xandra.{ConnectionError, Error, Frame}

  @spec recv_frame(:gen_tcp | :ssl, term, module, nil | module) ::
          {:ok, Frame.t()} | {:error, :closed | :inet.posix()}
  def recv_frame(transport, socket, protocol_module, compressor \\ nil)
      when is_atom(protocol_module) and is_atom(compressor) do
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
      |> Frame.encode(tentative_protocol_module)

    with :ok <- transport.send(socket, payload),
         {:ok, %Frame{} = frame} <-
           recv_frame(transport, socket, tentative_protocol_module, compressor) do
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
      |> Frame.encode(protocol_module)

    # However, we need to pass the compressor module around when we
    # receive the response to this frame because if we said we want to use
    # compression, this response is already compressed.
    with :ok <- transport.send(socket, payload),
         {:ok, frame} <- recv_frame(transport, socket, protocol_module, compressor) do
      # TODO: handle :error frames for things like :protocol_violation.
      case frame do
        %Frame{body: <<>>} ->
          :ok

        %Frame{kind: :authenticate} ->
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

    with :ok <- transport.send(socket, payload),
         {:ok, frame} <- recv_frame(transport, socket, protocol_module, compressor) do
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
