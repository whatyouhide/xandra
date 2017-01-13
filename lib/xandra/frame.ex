defmodule Xandra.Frame do
  defstruct [:kind, :compression, :body, stream_id: 0, tracing: false]

  @type kind :: :startup | :options | :query | :prepare | :execute | :batch

  @type t(kind) :: %__MODULE__{kind: kind}
  @type t :: t(kind)

  @request_version 0x03

  @request_opcodes %{
    :startup => 0x01,
    :options => 0x05,
    :query => 0x07,
    :prepare => 0x09,
    :execute => 0x0A,
    :batch => 0x0D,
  }

  @response_version 0x83

  @response_opcodes %{
    0x00 => :error,
    0x02 => :ready,
    0x06 => :supported,
    0x08 => :result,
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

  @spec encode(t(kind)) :: binary
  def encode(%__MODULE__{} = frame) do
    %{compression: compression, tracing: tracing?,
      kind: kind, stream_id: stream_id, body: body} = frame
    opcode = Map.fetch!(@request_opcodes, kind)
    flags = encode_flags(compression, tracing?)
    body = maybe_compress_body(compression, body)
    <<@request_version, flags, stream_id::16, opcode, byte_size(body)::32, body::bytes>>
  end

  @spec decode(binary, binary) :: t(kind)
  def decode(header, body \\ <<>>) when is_binary(body) do
    <<@response_version, _flags, _stream_id::16, opcode, _::32>> = header
    kind = Map.fetch!(@response_opcodes, opcode)
    %__MODULE__{kind: kind, body: body}
  end

  defp encode_flags(nil, false), do: 0x00
  defp encode_flags(nil, true), do: 0x02
  defp encode_flags(_, false), do: 0x01
  defp encode_flags(_, true), do: 0x03

  defp maybe_compress_body(nil, body), do: body
end
