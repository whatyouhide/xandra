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
    assert_not_a_variable(buffer)

    quote do
      <<size::16, unquote(value)::size(size)-bytes, unquote(buffer)::bits>> = unquote(buffer)
    end
  end

  # Decodes a "uuid".
  defmacro decode_uuid({:<-, _, [value, buffer]}) do
    assert_not_a_variable(buffer)

    quote do
      <<unquote(value)::16-bytes, unquote(buffer)::bits>> = unquote(buffer)
    end
  end

  @spec date_from_unix_days(integer()) :: Calendar.date()
  def date_from_unix_days(days) when is_integer(days) do
    Date.add(~D[1970-01-01], days)
  end

  @spec date_to_unix_days(Calendar.date()) :: integer()
  def date_to_unix_days(date) do
    Date.diff(date, ~D[1970-01-01])
  end

  @spec time_from_nanoseconds(integer()) :: Calendar.time()
  def time_from_nanoseconds(nanoseconds) when is_integer(nanoseconds) do
    Time.add(~T[00:00:00], nanoseconds, :nanosecond)
  end

  @spec time_to_nanoseconds(Calendar.time()) :: integer()
  def time_to_nanoseconds(time) do
    Time.diff(time, ~T[00:00:00.000000], :nanosecond)
  end

  defp assert_not_a_variable(ast) do
    if not match?({var, _context, nil} when is_atom(var), ast) do
      raise ArgumentError,
            "the right-hand side of <- must be a variable, got: #{Macro.to_string(ast)}"
    end
  end
end
