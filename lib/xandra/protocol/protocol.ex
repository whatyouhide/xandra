defmodule Xandra.Protocol do
  @moduledoc false

  import Bitwise

  @valid_flag_bits for shift <- 0..7, do: 1 <<< shift
  @flag_mask_range 0x00..0xFF

  @type flag_mask() :: 0x00..0xFF
  @type flag_bit() ::
          unquote(Enum.reduce(@valid_flag_bits, &quote(do: unquote(&1) | unquote(&2))))

  defp assert_not_a_variable(ast) do
    if not match?({var, _context, nil} when is_atom(var), ast) do
      raise ArgumentError,
            "the right-hand side of <- must be a variable, got: #{Macro.to_string(ast)}"
    end
  end

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

  defmacro decode_value({:<-, _, [value, buffer]}, type, do: block) do
    assert_not_a_variable(buffer)

    quote do
      <<size::32-signed, unquote(buffer)::bits>> = unquote(buffer)

      if size < 0 do
        unquote(value) = nil
        unquote(block)
      else
        <<data::size(size)-bytes, unquote(buffer)::bits>> = unquote(buffer)
        unquote(value) = decode_value(data, unquote(type))
        unquote(block)
      end
    end
  end

  # Decodes a "uuid".
  defmacro decode_uuid({:<-, _, [value, buffer]}) do
    assert_not_a_variable(buffer)

    quote do
      <<unquote(value)::16-bytes, unquote(buffer)::bits>> = unquote(buffer)
    end
  end

  # A [short] n, followed by n [string].
  # https://github.com/apache/cassandra/blob/ce4ae43a310a809fb0c82a7f48001a0f8206e156/doc/native_protocol_v5.spec#L383
  @spec decode_string_list(bitstring()) :: {[String.t()], bitstring()}
  def decode_string_list(<<count::16, buffer::bits>>) do
    decode_string_list(buffer, count, [])
  end

  defp decode_string_list(<<buffer::bits>>, 0, acc) do
    {Enum.reverse(acc), buffer}
  end

  defp decode_string_list(<<buffer::bits>>, count, acc) do
    decode_string(item <- buffer)
    decode_string_list(buffer, count - 1, [item | acc])
  end

  # Only used in native protocol v4+.
  @spec decode_warnings(bitstring(), boolean()) :: {[String.t()], bitstring()}
  def decode_warnings(body, _warning? = false), do: {[], body}
  def decode_warnings(body, _warning? = true), do: decode_string_list(body)

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

  @spec set_flag(flag_mask(), pos_integer(), term()) :: flag_mask()
  def set_flag(bitmask, flag_bit, value_present)
      when is_integer(bitmask) and bitmask in @flag_mask_range and is_integer(flag_bit) and
             flag_bit in @valid_flag_bits do
    if value_present do
      bitmask ||| flag_bit
    else
      bitmask
    end
  end
end
