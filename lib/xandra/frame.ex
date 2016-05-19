defmodule Xandra.Frame do
  defstruct [:opcode, :compression, stream_id: 0, tracing: false]

  @request_version 0x03

  def encode(%__MODULE__{} = frame, body \\ "") do
    %{stream_id: stream_id, opcode: opcode,
      compression: compression, tracing: tracing} = frame
    flags = encode_flags(compression, tracing)
    body = maybe_compress_body(compression, body)
    <<@request_version, flags, stream_id::16, opcode, byte_size(body)::32, body::bytes>>
  end

  defp encode_flags(nil, false), do: 0x00
  defp encode_flags(nil, true), do: 0x02
  defp encode_flags(_, false), do: 0x01
  defp encode_flags(_, true), do: 0x03

  defp maybe_compress_body(nil, body), do: body
end
