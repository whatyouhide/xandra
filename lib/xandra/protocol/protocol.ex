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

  @spec supports_custom_payload?(module()) :: boolean()
  def supports_custom_payload?(protocol_module)

  def supports_custom_payload?(Xandra.Protocol.V3), do: false
  def supports_custom_payload?(Xandra.Protocol.V4), do: true
  def supports_custom_payload?(Xandra.Protocol.V5), do: true

  # Main macro to decode from a type "specification" like the ones found in the
  # docs for the C* native protocols. For example, [short] or [bytes map].
  defmacro decode_from_proto_type({:<-, _meta, [value, buffer]}, proto_type) do
    assert_not_a_variable(buffer)
    decode_from_type(value, buffer, proto_type)
  end

  # Variant of the decode_from_proto_type/2 that takes a block of code. This is done
  # in case there is conditional binding of the buffer, so that the binary context
  # is kept.
  defmacro decode_from_proto_type({:<-, _meta, [value, buffer]}, proto_type, do: block) do
    assert_not_a_variable(buffer)
    decode_from_type(value, buffer, proto_type, block)
  end

  # A 4 bytes integer
  defp decode_from_type(value, buffer, "[int]") do
    quote do: <<unquote(value)::32-signed, unquote(buffer)::bits>> = unquote(buffer)
  end

  # A 8 bytes integer
  defp decode_from_type(value, buffer, "[long]") do
    quote do: <<unquote(value)::64, unquote(buffer)::bits>> = unquote(buffer)
  end

  # A 1 byte unsigned integer
  defp decode_from_type(value, buffer, "[byte]") do
    quote do: <<unquote(value)::8-unsigned, unquote(buffer)::bits>> = unquote(buffer)
  end

  # A 2 bytes unsigned integer
  defp decode_from_type(value, buffer, "[short]") do
    quote do: <<unquote(value)::16-unsigned, unquote(buffer)::bits>> = unquote(buffer)
  end

  # A [short] n, followed by n bytes representing an UTF-8 string.
  defp decode_from_type(value, buffer, "[string]") do
    size = Macro.var(:size, __MODULE__)

    quote do
      unquote(decode_from_type(size, buffer, "[short]"))
      <<unquote(value)::bytes-size(unquote(size)), unquote(buffer)::bits>> = unquote(buffer)
    end
  end

  # An [int] n, followed by n bytes representing an UTF-8 string.
  defp decode_from_type(value, buffer, "[long string]") do
    quote do
      unquote(decode_from_type(value, buffer, "[int]"))
      <<unquote(value)::size(value), unquote(buffer)::bits>> = unquote(buffer)
    end
  end

  # A 16 bytes long uuid.
  defp decode_from_type(value, buffer, "[uuid]") do
    quote do
      <<unquote(value)::16-bytes, unquote(buffer)::bits>> = unquote(buffer)
    end
  end

  # A [short] n, followed by n [string].
  defp decode_from_type(value, buffer, "[string list]") do
    quote do
      unquote(decode_from_type(value, buffer, "[short]"))

      {unquote(value), unquote(buffer)} =
        unquote(__MODULE__).decode_string_list(
          unquote(buffer),
          _count = unquote(value),
          _acc = []
        )
    end
  end

  # A [short] n, followed by n pair <k><v> where <k> is a [string] and <v> is a [bytes].
  defp decode_from_type(value, buffer, "[bytes map]") do
    count = Macro.var(:count, __MODULE__)

    quote do
      unquote(decode_from_type(count, buffer, "[short]"))

      {unquote(value), unquote(buffer)} =
        unquote(__MODULE__).decode_bytes_map_pairs(unquote(buffer), unquote(count), _acc = [])
    end
  end

  # A [short] n, followed by n pair <k><v> where <k> is a [string] and <v> is a [string list].
  defp decode_from_type(value, buffer, "[string multimap]") do
    count = Macro.var(:count, __MODULE__)

    quote do
      unquote(decode_from_type(count, buffer, "[short]"))

      {unquote(value), unquote(buffer)} =
        unquote(__MODULE__).decode_string_multimap_pairs(
          unquote(buffer),
          unquote(count),
          _acc = []
        )
    end
  end

  # A [int] n, followed by n bytes if n >= 0. If n < 0, no byte should follow and
  # the value represented is `null`.
  defp decode_from_type(value, buffer, "[bytes]", block) do
    size = Macro.var(:size, __MODULE__)

    quote do
      unquote(decode_from_type(size, buffer, "[int]"))

      if unquote(size) >= 0 do
        <<unquote(value)::size(unquote(size))-bytes, unquote(buffer)::bits>> = unquote(buffer)
        unquote(block)
      else
        unquote(value) = nil
        unquote(block)
      end
    end
  end

  # A [int] n, followed by n bytes if n >= 0.
  # If n == -1 no byte should follow and the value represented is `null`.
  # If n == -2 no byte should follow and the value represented is
  # `not set` not resulting in any change to the existing value.
  # n < -2 is an invalid value and results in an error.
  defp decode_from_type(value, buffer, "[value]", block) do
    int = Macro.var(:int, __MODULE__)

    quote do
      unquote(decode_from_type(int, buffer, "[int]"))

      if unquote(int) < 0 do
        unquote(value) = nil
        unquote(block)
      else
        <<unquote(value)::bytes-size(unquote(int)), unquote(buffer)::bits>> = unquote(buffer)
        unquote(block)
      end
    end
  end

  # Helper to decode a list of pairs for a [bytes map].
  @spec decode_bytes_map_pairs(bitstring(), non_neg_integer(), [{String.t(), binary()}]) ::
          {%{optional(String.t()) => binary()}, bitstring()}
  def decode_bytes_map_pairs(<<buffer::bits>>, 0, acc) do
    {Map.new(acc), buffer}
  end

  def decode_bytes_map_pairs(<<buffer::bits>>, count, acc) do
    decode_from_proto_type(key <- buffer, "[string]")

    decode_from_proto_type(value <- buffer, "[bytes]") do
      decode_bytes_map_pairs(buffer, count - 1, [{key, value} | acc])
    end
  end

  # Helper to decode a list of pairs for a [string multimap].
  @spec decode_bytes_map_pairs(bitstring(), non_neg_integer(), [{String.t(), [String.t()]}]) ::
          {%{optional(String.t()) => [String.t()]}, bitstring()}
  def decode_string_multimap_pairs(<<buffer::bits>>, 0, acc) do
    {Map.new(acc), buffer}
  end

  def decode_string_multimap_pairs(<<buffer::bits>>, count, acc) do
    decode_from_proto_type(key <- buffer, "[string]")
    decode_from_proto_type(value <- buffer, "[string list]")
    decode_string_multimap_pairs(buffer, count - 1, [{key, value} | acc])
  end

  @spec decode_string_list(bitstring(), non_neg_integer(), [String.t()]) ::
          {[String.t()], bitstring()}
  def decode_string_list(<<buffer::bits>>, 0, acc) do
    {Enum.reverse(acc), buffer}
  end

  def decode_string_list(<<buffer::bits>>, count, acc) do
    decode_from_proto_type(item <- buffer, "[string]")
    decode_string_list(buffer, count - 1, [item | acc])
  end

  # Only used in native protocol v4+.
  @spec decode_warnings(bitstring(), boolean()) :: {[String.t()], bitstring()}
  def decode_warnings(body, _warning? = false) do
    {[], body}
  end

  def decode_warnings(body, _warning? = true) do
    decode_from_proto_type(warnings <- body, "[string list]")
    {warnings, body}
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
