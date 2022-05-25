defmodule Xandra.Protocol do
  @moduledoc false

  # Takes a protocol module and returns the protocol "format", that is, whether frames (= envelopes) should be wrapped inside the v5+ frame wrapper or not.
  @spec frame_protocol_format(module()) :: :v4_or_less | :v5_or_more
  def frame_protocol_format(protocol_module)

  def frame_protocol_format(Xandra.Protocol.V3), do: :v4_or_less
  def frame_protocol_format(Xandra.Protocol.V4), do: :v4_or_less
  def frame_protocol_format(Xandra.Protocol.V5), do: :v5_or_more

  # Decodes a "string" as per
  # https://github.com/apache/cassandra/blob/dcf3d58c4b22b8b69e8505b170829172ea3c4f5c/doc/native_protocol_v5.spec#L361
  # > "A [short] n, followed by n bytes representing an UTF-8 string."
  defmacro decode_string({:<-, _, [value, buffer]}) do
    if not match?({var, _context, nil} when is_atom(var), buffer) do
      raise ArgumentError,
            "the right-hand side of <- must be a variable, got: #{Macro.to_string(buffer)}"
    end

    quote do
      <<size::16, unquote(value)::size(size)-bytes, unquote(buffer)::bits>> = unquote(buffer)
    end
  end
end
