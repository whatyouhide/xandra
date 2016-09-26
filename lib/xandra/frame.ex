defmodule Xandra.Frame do
  defstruct [:opcode, :compression, :body, stream_id: 0, tracing: false]

  @request_version 0x03

  @opcodes %{
    :startup => 0x01,
    :options => 0x05,
    :query => 0x07,
    :prepare => 0x09,
    :execute => 0x0A,
  }

  def new(operation, body \\ <<>>) do
    opcode = Map.fetch!(@opcodes, operation)
    %__MODULE__{opcode: opcode, body: body}
  end

  def header_length(), do: 9

  def body_length(<<_::5-bytes, length::32>>) do
    length
  end

  def encode(%__MODULE__{} = frame) do
    %{compression: compression, tracing: tracing?,
      opcode: opcode, stream_id: stream_id, body: body} = frame
    flags = encode_flags(compression, tracing?)
    body = maybe_compress_body(compression, body)
    <<@request_version, flags, stream_id::16, opcode, byte_size(body)::32, body::bytes>>
  end

  def decode(header, body \\ <<>>) do
    <<_cql_version, _flags, _stream_id::16, opcode, _::32>> = header
    %__MODULE__{opcode: opcode, body: body}
  end

  defp encode_flags(nil, false), do: 0x00
  defp encode_flags(nil, true), do: 0x02
  defp encode_flags(_, false), do: 0x01
  defp encode_flags(_, true), do: 0x03

  defp maybe_compress_body(nil, body), do: body
end
