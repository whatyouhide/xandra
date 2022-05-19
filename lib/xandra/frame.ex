defmodule Xandra.Frame do
  @moduledoc false

  defstruct [
    :kind,
    :body,
    :protocol_version,
    stream_id: 0,
    compressor: nil,
    tracing: false,
    warning: false,
    custom_payload: false,
    atom_keys?: false
  ]

  use Bitwise

  alias Xandra.Protocol.CRC

  @supported_protocols [
    v4: %{module: Xandra.Protocol.V4, request_version: 0x04, response_version: 0x84},
    # For now, v5 isn't top in the list because automatic protocol negotiation should still
    # use v4 until v5 has complete support. This still allows you to force the protocol
    # to v5 with protocol_version: :v5.
    # TODO: move this to the top once protocol v5 is stable.
    v5: %{module: Xandra.Protocol.V5, request_version: 0x05, response_version: 0x85},
    v3: %{module: Xandra.Protocol.V3, request_version: 0x03, response_version: 0x83}
  ]

  @supported_protocol_versions for {vsn, _} <- @supported_protocols, do: vsn

  @type kind ::
          :startup
          | :options
          | :query
          | :prepare
          | :execute
          | :register
          | :batch
          | :auth_response
          | :error
          | :ready
          | :authenticate
          | :supported
          | :result
          | :event
          | :auth_success

  @type t(kind) :: %__MODULE__{kind: kind}
  @type t :: t(kind)

  @type supported_protocol ::
          unquote(
            Enum.reduce(@supported_protocol_versions, &quote(do: unquote(&1) | unquote(&2)))
          )

  @opcodes [
    # Requests
    startup: 0x01,
    options: 0x05,
    query: 0x07,
    prepare: 0x09,
    execute: 0x0A,
    register: 0x0B,
    batch: 0x0D,
    auth_response: 0x0F,

    # Responses
    error: 0x00,
    ready: 0x02,
    authenticate: 0x03,
    supported: 0x06,
    result: 0x08,
    event: 0x0C,
    auth_success: 0x10
  ]

  @max_v5_payload_size_in_bytes 128 * 1024 - 1

  for {human_code, opcode} <- @opcodes do
    defp opcode_for_op(unquote(human_code)), do: unquote(opcode)
    defp op_for_opcode(unquote(opcode)), do: unquote(human_code)
  end

  ## Functions related to the native protocol version.

  @spec max_supported_protocol() :: unquote(hd(@supported_protocol_versions))
  def max_supported_protocol(), do: unquote(hd(@supported_protocol_versions))

  @spec supported_protocols() :: [supported_protocol, ...]
  def supported_protocols(), do: @supported_protocol_versions

  for {vsn,
       %{
         module: protocol_mod,
         request_version: request_vsn,
         response_version: response_vsn
       }} <- @supported_protocols do
    defp protocol_module_to_request_version(unquote(protocol_mod)), do: unquote(request_vsn)
    defp protocol_module_to_response_version(unquote(protocol_mod)), do: unquote(response_vsn)
    defp byte_version_to_protocol_version(unquote(request_vsn)), do: unquote(vsn)
    defp byte_version_to_protocol_version(unquote(response_vsn)), do: unquote(vsn)
  end

  @spec protocol_version_to_module(supported_protocol) :: module
  def protocol_version_to_module(version)

  for {vsn, %{module: mod}} <- @supported_protocols do
    def protocol_version_to_module(unquote(vsn)), do: unquote(mod)
  end

  ## Frame functions.

  @spec new(kind, keyword) :: t(kind) when kind: var
  def new(kind, options \\ []) do
    %__MODULE__{
      kind: kind,
      compressor: Keyword.get(options, :compressor),
      tracing: Keyword.get(options, :tracing, false)
    }
  end

  @spec header_length() :: 9
  def header_length(), do: 9

  @spec body_length(binary) :: non_neg_integer
  def body_length(<<_::5-bytes, length::32>>) do
    length
  end

  # Encodes a frame into an "envelope", that is, the single encoded frame in protocol v4 or less.
  # This can be called even with protocol_module set to Xandra.Protocol.V5 or more, because
  # frames are encoded in the same way *inside* the outer v5+ framing.
  @spec encode_v4(t(kind), module) :: iodata
  def encode_v4(%__MODULE__{} = frame, protocol_module) when is_atom(protocol_module) do
    %__MODULE__{
      compressor: compressor,
      tracing: tracing?,
      kind: kind,
      stream_id: stream_id,
      body: body
    } = frame

    body = maybe_compress_body(compressor, body)

    [
      protocol_module_to_request_version(protocol_module),
      encode_flags(compressor, tracing?),
      <<stream_id::16>>,
      opcode_for_op(kind),
      <<IO.iodata_length(body)::32>>,
      body
    ]
  end

  @spec encode(t(kind), module) :: iodata
  def encode(frame, protocol_module)

  def encode(%__MODULE__{} = frame, Xandra.Protocol.V5 = protocol_module) do
    if not is_nil(frame.compressor) do
      raise "Xandra doesn't support compression in native protocol v5 yet"
    end

    frame
    |> encode_v4(protocol_module)
    |> encode_v5_wrappers()
  end

  def encode(%__MODULE__{} = frame, protocol_module) when is_atom(protocol_module) do
    encode_v4(frame, protocol_module)
  end

  # Made public for testing.
  @doc false
  @spec encode_v5_wrappers(iodata()) :: iodata()
  def encode_v5_wrappers(frame_iodata) do
    payload_length = IO.iodata_length(frame_iodata)

    # Small optimization: if the payload is smaller than the max size, then we don't need to
    # convert it to a binary in order to chunk it, and we can leave it as iodata. This should
    # be the most common case for most payloads, so it should be worth to do this.
    segments =
      if payload_length <= @max_v5_payload_size_in_bytes do
        [frame_iodata]
      else
        frame_iodata
        |> IO.iodata_to_binary()
        |> chunk_binary(@max_v5_payload_size_in_bytes, _acc = [])
      end

    encode_v5_wrappers_for_segments(segments)
  end

  defp encode_v5_wrappers_for_segments(segments) when is_list(segments) do
    self_contained? = match?([_], segments)

    for segment <- segments do
      buffer = <<>>
      payload_length = IO.iodata_length(segment)

      buffer = encode_v5_header(buffer, payload_length, self_contained?)
      buffer = [buffer, segment]

      payload_crc = CRC.crc32(segment)
      [buffer, <<payload_crc::4-unit(8)-unsigned-little>>]
    end
  end

  defp encode_v5_header(buffer, payload_length, self_contained?) do
    header_data = payload_length

    header_data =
      if self_contained? do
        header_data ||| 1 <<< 17
      else
        header_data
      end

    header_data_as_binary = <<header_data::3-unit(8)-unsigned-little>>
    header_crc = CRC.crc24(header_data_as_binary)

    [buffer, header_data_as_binary, <<header_crc::3-unit(8)-unsigned-little>>]
  end

  ## Decoding

  @spec decode_v4(
          (fetch_state, pos_integer() -> {:ok, binary(), fetch_state} | {:error, reason}),
          fetch_state,
          module() | nil
        ) :: {:ok, binary()} | {:error, reason}
        when fetch_state: term(), reason: term()
  def decode_v4(fetch_bytes_fun, fetch_state, compressor)
      when is_function(fetch_bytes_fun, 2) and is_atom(compressor) do
    length = header_length()

    with {:ok, header, fetch_state} <- fetch_bytes_fun.(fetch_state, length) do
      case body_length(header) do
        0 ->
          {:ok, decode(header)}

        body_length ->
          with {:ok, body, _bytes_state} <- fetch_bytes_fun.(fetch_state, body_length),
               do: {:ok, decode(header, body, compressor)}
      end
    end
  end

  @spec decode_v5(
          (fetch_state, pos_integer() -> {:ok, binary(), fetch_state} | {:error, reason}),
          fetch_state,
          module() | nil
        ) :: {:ok, binary()} | {:error, reason}
        when fetch_state: term(), reason: term()
  def decode_v5(fetch_bytes_fun, fetch_state, compressor)
      when is_function(fetch_bytes_fun, 2) and is_atom(compressor) do
    if not is_nil(compressor) do
      raise "Xandra doesn't support compression in native protocol v5 yet"
    end

    with {:ok, envelope} <- decode_v5_wrapper(fetch_bytes_fun, fetch_state) do
      {frame, ""} = decode_from_binary(envelope, compressor)
      {:ok, frame}
    end
  end

  # Made public for testing.
  @doc false
  def decode_v5_wrapper(fetch_bytes_fun, fetch_state) do
    decode_v5_wrapper(fetch_bytes_fun, fetch_state, _payload_acc = <<>>)
  end

  defp decode_v5_wrapper(fetch_bytes_fun, fetch_state, payload_acc)
       when is_function(fetch_bytes_fun, 2) do
    with {:ok, header, fetch_state} <- fetch_bytes_fun.(fetch_state, _byte_count = 6) do
      <<header_contents::3-bytes, crc24_of_header::3-unit(8)-integer-little>> = header

      <<header_data::3-unit(8)-integer-little>> = header_contents

      if crc24_of_header != CRC.crc24(header_contents) do
        raise "mismatching CRC24 for header"
      end

      # Cap the payload length to a max of 17 bits from header_contents.
      payload_length = header_data &&& @max_v5_payload_size_in_bytes

      # The self-contained flag is the 18th bit.
      self_contained? = (header_data >>> 17 &&& 1) == 1

      case fetch_bytes_fun.(fetch_state, payload_length + 4) do
        {:ok, <<payload::size(payload_length)-bytes, crc32_of_payload::32-integer-little>>,
         fetch_state} ->
          if crc32_of_payload != CRC.crc32(payload) do
            raise "mismatching CRC32 for payload"
          end

          if self_contained? or payload_length < @max_v5_payload_size_in_bytes do
            {:ok, payload_acc <> payload}
          else
            decode_v5_wrapper(fetch_bytes_fun, fetch_state, payload_acc <> payload)
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # Used to decode a frame from a whole binary instead of from "pieces" of a binary.
  # This is used when we get the whole frame inside an envelope (for protocol v5+).
  # Protocols v4- use a different approach (they fetch some bytes from the wire for the header,
  # decode them to figure out the length of the body, fetch that amount of bytes from the
  # wire, and so on).
  @spec decode_from_binary(binary, module | nil) :: t(kind)
  def decode_from_binary(binary, compressor) when is_binary(binary) and is_atom(compressor) do
    header_length = header_length()

    <<header::size(header_length)-binary, rest::binary>> = binary

    body_length = body_length(header)
    <<body::size(body_length)-binary, rest::binary>> = rest

    {decode(header, body, compressor), rest}
  end

  @spec decode(binary, binary, nil | module) :: t(kind)
  def decode(header, body \\ <<>>, compressor \\ nil)
      when is_binary(header) and is_binary(body) and is_atom(compressor) do
    <<response_version, flags, stream_id::16, opcode, _::32>> = header

    compression? = flag_set?(flags, _compression = 0x01)
    tracing? = flag_set?(flags, _tracing = 0x02)
    custom_payload? = flag_set?(flags, _custom_payload = 0x04)
    warning? = flag_set?(flags, _warning? = 0x08)

    kind = op_for_opcode(opcode)
    body = maybe_decompress_body(compression?, compressor, body)

    %__MODULE__{
      kind: kind,
      body: body,
      stream_id: stream_id,
      protocol_version: byte_version_to_protocol_version(response_version),
      tracing: tracing?,
      warning: warning?,
      custom_payload: custom_payload?,
      compressor: compressor
    }
  end

  defp encode_flags(_compressor = nil, _tracing? = false), do: 0x00
  defp encode_flags(_compressor = nil, _tracing? = true), do: 0x02
  defp encode_flags(_compressor = _, _tracing? = false), do: 0x01
  defp encode_flags(_compressor = _, _tracing? = true), do: 0x03

  defp flag_set?(flags, flag) do
    (flags &&& flag) == flag
  end

  defp maybe_compress_body(_compressor = nil, body), do: body
  defp maybe_compress_body(compressor, body), do: compressor.compress(body)

  defp maybe_decompress_body(_compression? = true, _compressor = nil, _body) do
    raise "received frame was flagged as compressed, but there's no module to decompress"
  end

  defp maybe_decompress_body(_compression? = true, compressor, body) do
    compressor.decompress(body)
  end

  defp maybe_decompress_body(_compression? = false, _compressor, body) do
    body
  end

  defp chunk_binary(binary, chunk_size, acc) do
    case binary do
      <<chunk::size(chunk_size)-binary, rest::binary>> ->
        chunk_binary(rest, chunk_size, [chunk | acc])

      <<chunk::binary>> when byte_size(chunk) < chunk_size ->
        Enum.reverse([chunk | acc])
    end
  end
end
