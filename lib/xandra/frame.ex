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
    use_beta: false,
    atom_keys?: false
  ]

  import Bitwise

  alias Xandra.Protocol.CRC

  # This list must be ordered: the first item is the "max supported protocol" that
  # Xandra will attempt to use on new connections.
  @supported_protocols [
    v5: %{module: Xandra.Protocol.V5, request_version: 0x05, response_version: 0x85},
    v4: %{module: Xandra.Protocol.V4, request_version: 0x04, response_version: 0x84},
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

  @flags [
    compression: 0x01,
    tracing: 0x02,
    custom_payload: 0x04,
    warning: 0x08,
    use_beta: 0x10
  ]

  @max_v5_payload_size_in_bytes 128 * 1024 - 1
  @v5_payload_length_bits 17
  @v5_header_crc_bytes 3

  for {human_code, opcode} <- @opcodes do
    defp opcode_for_op(unquote(human_code)), do: unquote(opcode)
    defp op_for_opcode(unquote(opcode)), do: unquote(human_code)
  end

  for {name, mask} <- @flags do
    defp flag_mask(unquote(name)), do: unquote(mask)
    defp flag_set?(byte, unquote(name)), do: (byte &&& unquote(mask)) == unquote(mask)
  end

  ## Functions related to the native protocol version.

  @spec max_supported_protocol() :: unquote(hd(@supported_protocol_versions))
  def max_supported_protocol(), do: hd(@supported_protocol_versions)

  @spec supported_protocols() :: [supported_protocol, ...]
  def supported_protocols(), do: @supported_protocol_versions

  @spec previous_protocol(supported_protocol) :: supported_protocol
  def previous_protocol(protocol_version)

  for {vsn, previous_vsn} <-
        Enum.zip(@supported_protocol_versions, Enum.drop(@supported_protocol_versions, 1)) do
    def previous_protocol(unquote(vsn)), do: unquote(previous_vsn)
  end

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
  def new(kind, options) when is_list(options) do
    %__MODULE__{
      kind: kind,
      compressor: Keyword.get(options, :compressor),
      tracing: Keyword.get(options, :tracing, false),
      custom_payload: is_map(options[:custom_payload])
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
      use_beta: use_beta?,
      custom_payload: custom_payload?,
      kind: kind,
      stream_id: stream_id,
      body: body
    } = frame

    # We should only set the CUSTOM_PAYLOAD flag if the native protocol version we're
    # using supports it.
    custom_payload? =
      custom_payload? and Xandra.Protocol.supports_custom_payload?(protocol_module)

    body = maybe_compress_body(compressor, body)

    flags =
      [
        tracing? && :tracing,
        compressor && :compression,
        use_beta? && :use_beta,
        custom_payload? && :custom_payload
      ]
      |> Enum.filter(& &1)
      |> encode_flags()

    [
      protocol_module_to_request_version(protocol_module),
      flags,
      <<stream_id::16>>,
      opcode_for_op(kind),
      <<IO.iodata_length(body)::32>>,
      body
    ]
  end

  @spec encode(t(kind), module) :: iodata
  def encode(frame, protocol_module)

  def encode(%__MODULE__{} = frame, Xandra.Protocol.V5 = protocol_module) do
    # We have to remove the compressor before encoding for protocol v4,
    # otherwise we encode the inner frame. We do the compression when we encode
    # the outer frame with the protocol v5 format.
    {compressor, frame} = get_and_update_in(frame.compressor, &{&1, nil})

    frame
    |> encode_v4(protocol_module)
    |> encode_v5_wrappers(compressor)
  end

  def encode(%__MODULE__{} = frame, protocol_module) when is_atom(protocol_module) do
    encode_v4(frame, protocol_module)
  end

  # Made public for testing.
  @doc false
  @spec encode_v5_wrappers(iodata(), module() | nil) :: iodata()
  def encode_v5_wrappers(frame_iodata, compressor) when is_atom(compressor) do
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

    encode_v5_wrappers_for_segments(segments, compressor)
  end

  defp encode_v5_wrappers_for_segments(segments, compressor) when is_list(segments) do
    self_contained_flag = if match?([_], segments), do: 1, else: 0
    Enum.map(segments, &encode_v5_segment(&1, self_contained_flag, compressor))
  end

  defp encode_v5_segment(segment, self_contained_flag, _compressor = nil) do
    uncompressed_payload_length = IO.iodata_length(segment)

    [
      encode_v5_header_uncompressed(uncompressed_payload_length, self_contained_flag),
      segment,
      <<CRC.crc32(segment)::4-unit(8)-unsigned-little>>
    ]
  end

  defp encode_v5_segment(segment, self_contained_flag, compressor) when is_atom(compressor) do
    uncompressed_payload_length = IO.iodata_length(segment)

    <<^uncompressed_payload_length::32-big-unsigned, compressed_payload::binary>> =
      compressor.compress(segment) |> IO.iodata_to_binary()

    [
      encode_v5_header_compressed(
        _compressed_payload_length = byte_size(compressed_payload),
        uncompressed_payload_length,
        self_contained_flag
      ),
      compressed_payload,
      <<CRC.crc32(compressed_payload)::4-unit(8)-unsigned-little>>
    ]
  end

  defp encode_v5_header_uncompressed(payload_length, self_contained_flag) do
    header_data = payload_length ||| self_contained_flag <<< @v5_payload_length_bits

    header_data_as_binary = <<header_data::3-unit(8)-unsigned-little>>
    [header_data_as_binary | <<CRC.crc24(header_data_as_binary)::3-unit(8)-unsigned-little>>]
  end

  defp encode_v5_header_compressed(
         compressed_payload_length,
         uncompressed_payload_length,
         self_contained_flag
       ) do
    header_data =
      compressed_payload_length ||| uncompressed_payload_length <<< @v5_payload_length_bits

    header_data = header_data ||| self_contained_flag <<< (@v5_payload_length_bits * 2)

    header_data_as_binary = <<header_data::5-unit(8)-unsigned-little>>
    [header_data_as_binary | <<CRC.crc24(header_data_as_binary)::3-unit(8)-unsigned-little>>]
  end

  ## Decoding

  @spec decode_v4(
          (fetch_state, pos_integer() -> {:ok, binary(), fetch_state} | {:error, reason}),
          fetch_state,
          module() | nil
        ) :: {:ok, t()} | {:error, reason}
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
        ) :: {:ok, t()} | {:error, reason}
        when fetch_state: term(), reason: term()
  def decode_v5(fetch_bytes_fun, fetch_state, compressor)
      when is_function(fetch_bytes_fun, 2) and is_atom(compressor) do
    with {:ok, envelope} <- decode_v5_wrapper(fetch_bytes_fun, fetch_state, compressor) do
      {frame, ""} = decode_from_binary(envelope, compressor)
      {:ok, frame}
    end
  end

  # Made public for testing.
  @doc false
  def decode_v5_wrapper(fetch_bytes_fun, fetch_state, compressor) do
    case decode_next_v5_wrapper(fetch_bytes_fun, fetch_state, compressor) do
      {:done, payload, _fetch_state} ->
        {:ok, payload}

      {:not_self_contained, payload, fetch_state} ->
        v4_header_length = header_length()
        <<v4_header::size(v4_header_length)-bytes, v4_payload_start::binary>> = payload

        decode_v5_wrapper_not_self_contained(
          fetch_bytes_fun,
          fetch_state,
          compressor,
          _v4_frame_length = body_length(v4_header),
          _v4_fetched_body_bytes = byte_size(v4_payload_start),
          _payload_acc = payload
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_v5_wrapper_not_self_contained(
         fetch_bytes_fun,
         fetch_state,
         compressor,
         v4_frame_length,
         v4_fetched_body_bytes,
         payload_acc
       ) do
    if v4_fetched_body_bytes >= v4_frame_length do
      {:ok, IO.iodata_to_binary(payload_acc)}
    else
      case decode_next_v5_wrapper(fetch_bytes_fun, fetch_state, compressor) do
        {:error, reason} ->
          {:error, reason}

        {:not_self_contained, chunk, _fetch_state}
        when byte_size(chunk) + v4_fetched_body_bytes >= v4_frame_length ->
          {:ok, IO.iodata_to_binary([payload_acc | chunk])}

        {:not_self_contained, chunk, fetch_state} ->
          decode_v5_wrapper_not_self_contained(
            fetch_bytes_fun,
            fetch_state,
            compressor,
            v4_frame_length,
            v4_fetched_body_bytes + byte_size(chunk),
            [payload_acc | chunk]
          )
      end
    end
  end

  defp decode_next_v5_wrapper(fetch_bytes_fun, fetch_state, compressor)
       when is_function(fetch_bytes_fun, 2) do
    header_size_in_bytes = if compressor, do: 8, else: 6

    with {:ok, header, fetch_state} <- fetch_bytes_fun.(fetch_state, header_size_in_bytes) do
      header_contents_size = header_size_in_bytes - @v5_header_crc_bytes

      <<header_contents::size(header_contents_size)-bytes,
        crc24_of_header::@v5_header_crc_bytes-unit(8)-integer-little>> = header

      <<header_data::size(header_contents_size)-unit(8)-integer-little>> = header_contents

      if crc24_of_header != CRC.crc24(header_contents) do
        raise "mismatching CRC24 for header"
      end

      # Cap the payload length to a max of 17 bits from header_contents.
      payload_length = header_data &&& @max_v5_payload_size_in_bytes

      # Shift the first 17 bits.
      header_data = header_data >>> @v5_payload_length_bits

      {uncompressed_payload_length, header_data} =
        if compressor do
          {header_data &&& @max_v5_payload_size_in_bytes, header_data >>> @v5_payload_length_bits}
        else
          {nil, header_data}
        end

      self_contained? = (header_data &&& 1) == 1

      case fetch_bytes_fun.(fetch_state, payload_length + 4) do
        {:ok, <<payload::size(payload_length)-bytes, crc32_of_payload::32-integer-little>>,
         fetch_state} ->
          if crc32_of_payload != CRC.crc32(payload) do
            raise "mismatching CRC32 for payload"
          end

          # We should not try to decompress the payload if the uncompressed payload size
          # is 0, apparently, which happens for stuff like void results (from empirical
          # experience). See the Python driver as an example:
          # https://github.com/datastax/python-driver/blob/9a645c58ca0ec57f775251f94e55c30aa837b2ad/cassandra/segment.py#L221
          uncompressed_payload =
            if compressor && uncompressed_payload_length > 0 do
              compressor.decompress(
                <<uncompressed_payload_length::32-big-unsigned, payload::binary>>
              )
            else
              payload
            end

          if self_contained? do
            {:done, uncompressed_payload, fetch_state}
          else
            {:not_self_contained, uncompressed_payload, fetch_state}
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

    compression? = flag_set?(flags, :compression)

    kind = op_for_opcode(opcode)
    body = maybe_decompress_body(compression?, compressor, body)

    %__MODULE__{
      kind: kind,
      body: body,
      stream_id: stream_id,
      protocol_version: byte_version_to_protocol_version(response_version),
      tracing: flag_set?(flags, :tracing),
      warning: flag_set?(flags, :warning),
      custom_payload: flag_set?(flags, :custom_payload),
      use_beta: flag_set?(flags, :use_beta),
      compressor: compressor
    }
  end

  defp encode_flags(flags) do
    Enum.reduce(flags, 0x00, fn name, acc -> acc ||| flag_mask(name) end)
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
