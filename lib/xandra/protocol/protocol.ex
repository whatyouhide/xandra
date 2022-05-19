defmodule Xandra.Protocol do
  @moduledoc false

  # Takes a protocol module and returns the protocol "format", that is, whether frames (= envelopes) should be wrapped inside the v5+ frame wrapper or not.
  @spec frame_protocol_format(module()) :: :v4_or_less | :v5_or_more
  def frame_protocol_format(protocol_module)

  def frame_protocol_format(Xandra.Protocol.V3), do: :v4_or_less
  def frame_protocol_format(Xandra.Protocol.V4), do: :v4_or_less
  def frame_protocol_format(Xandra.Protocol.V5), do: :v5_or_more
end
