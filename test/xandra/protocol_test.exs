defmodule Xandra.ProtocolTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Xandra.Protocol

  describe "frame_protocol_format/1" do
    test "returns the right \"format\"" do
      assert frame_protocol_format(Xandra.Protocol.V3) == :v4_or_less
      assert frame_protocol_format(Xandra.Protocol.V4) == :v4_or_less
      assert frame_protocol_format(Xandra.Protocol.V5) == :v5_or_more
    end
  end

  describe "decode_from_proto_type/2 with [string]" do
    test "decodes a string and rebinds variables" do
      encoded = <<3::16, "foo"::binary, "rest"::binary>>

      decode_from_proto_type(contents <- encoded, "[string]")

      assert contents == "foo"
      assert encoded == "rest"
    end

    test "raises if the size doesn't match" do
      encoded = <<3::16, "a"::binary>>

      assert_raise MatchError, fn ->
        decode_from_proto_type(_ <- encoded, "[string]")
        _ = encoded
      end
    end

    test "raises a compile-time error on malformed arguments" do
      message = "the right-hand side of <- must be a variable, got: :not_a_var"

      assert_raise ArgumentError, message, fn ->
        Code.eval_quoted(
          quote do
            decode_from_proto_type(_ <- :not_a_var, "[string]")
          end
        )
      end

      message = "the right-hand side of <- must be a variable, got: hello()"

      assert_raise ArgumentError, message, fn ->
        Code.eval_quoted(
          quote do
            decode_from_proto_type(_ <- hello(), "[string]")
          end
        )
      end
    end
  end

  describe "decode_from_proto_type/2 with [string list]" do
    property "works for zero strings" do
      check all cruft <- bitstring(), max_runs: 5 do
        buffer = <<0::16, cruft::bits>>
        decode_from_proto_type(list <- buffer, "[string list]")
        assert buffer == cruft
        assert list == []
      end
    end

    test "decodes strings" do
      buffer = <<2::16, 3::16, "foo"::binary, 2::16, "ab"::binary, 1::1>>
      decode_from_proto_type(list <- buffer, "[string list]")
      assert buffer == <<1::1>>
      assert list == ["foo", "ab"]
    end
  end
end
