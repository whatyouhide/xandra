defmodule Xandra.Frame do
  @moduledoc false

  defstruct [:kind, :body, stream_id: 0, tracing: false, warning: false, atom_keys?: false]

  use Bitwise

  alias Xandra.Protocol

  @type kind :: :startup | :options | :query | :prepare | :execute | :batch

  @type t(kind) :: %__MODULE__{kind: kind}
  @type t :: t(kind)

  @request_versions %{
    Protocol.V3 => 0x03,
    Protocol.V4 => 0x04
  }

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

  @response_versions %{
    Protocol.V3 => 0x83,
    Protocol.V4 => 0x84
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

  @spec new(kind) :: t(kind) when kind: var
  def new(kind) do
    %__MODULE__{kind: kind}
  end

  @spec header_length() :: 9
  def header_length(), do: 9

  @spec body_length(binary) :: non_neg_integer
  def body_length(<<_::5-bytes, length::32>>) do
    length
  end

  @spec encode(t(kind), module, nil | module) :: iodata
  def encode(%__MODULE__{} = frame, protocol_module, compressor \\ nil)
      when is_atom(protocol_module) and is_atom(compressor) do
    %{tracing: tracing?, kind: kind, stream_id: stream_id, body: body} = frame
    body = maybe_compress_body(compressor, body)

    [
      Map.fetch!(@request_versions, protocol_module),
      encode_flags(compressor, tracing?),
      <<stream_id::16>>,
      Map.fetch!(@request_opcodes, kind),
      <<IO.iodata_length(body)::32>>,
      body
    ]
  end

  @spec decode(binary, binary, module, nil | module) :: t(kind)
  def decode(header, body \\ <<>>, protocol_module, compressor \\ nil)
      when is_binary(body) and is_atom(compressor) do
    <<response_version, flags, _stream_id::16, opcode, _::32>> = header

    # For now, raise if the response version doens't match the requested protocol
    # because we don't know how to deal with the mismatch.
    assert_response_version_matches_request(response_version, protocol_module)

    compression? = flag_set?(flags, _compression = 0x01)
    warning? = flag_set?(flags, _warning? = 0x08)

    kind = Map.fetch!(@response_opcodes, opcode)
    body = maybe_decompress_body(compression?, compressor, body)

    %__MODULE__{kind: kind, body: body, warning: warning?}
  end

  defp assert_response_version_matches_request(response_version, protocol_module) do
    case Map.fetch!(@response_versions, protocol_module) do
      ^response_version ->
        :ok

      other ->
        raise "response version #{inspect(other, base: :hex)} doesn't match the " <>
                "requested protocol (#{inspect(protocol_module)})"
    end
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
