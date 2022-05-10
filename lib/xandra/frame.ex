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
    atom_keys?: false
  ]

  use Bitwise

  @supported_protocols [
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

  @request_opcodes %{
    :startup => 0x01,
    :options => 0x05,
    :query => 0x07,
    :prepare => 0x09,
    :execute => 0x0A,
    :register => 0x0B,
    :batch => 0x0D,
    :auth_response => 0x0F
  }

  @response_opcodes %{
    0x00 => :error,
    0x02 => :ready,
    0x03 => :authenticate,
    0x06 => :supported,
    0x08 => :result,
    0x0C => :event,
    0x10 => :auth_success
  }

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
    defp response_version_to_protocol_version(unquote(response_vsn)), do: unquote(vsn)
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

  @spec encode(t(kind), module) :: iodata
  def encode(%__MODULE__{} = frame, protocol_module) when is_atom(protocol_module) do
    %{
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
      Map.fetch!(@request_opcodes, kind),
      <<IO.iodata_length(body)::32>>,
      body
    ]
  end

  @spec decode(binary, binary, nil | module) :: t(kind)
  def decode(header, body \\ <<>>, compressor \\ nil)
      when is_binary(header) and is_binary(body) and is_atom(compressor) do
    <<response_version, flags, _stream_id::16, opcode, _::32>> = header

    compression? = flag_set?(flags, _compression = 0x01)
    tracing? = flag_set?(flags, _tracing = 0x02)
    warning? = flag_set?(flags, _warning? = 0x08)

    kind = Map.fetch!(@response_opcodes, opcode)
    body = maybe_decompress_body(compression?, compressor, body)

    %__MODULE__{
      kind: kind,
      body: body,
      protocol_version: response_version_to_protocol_version(response_version),
      tracing: tracing?,
      warning: warning?,
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
end
