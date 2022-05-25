defmodule Xandra.ProtocolTest do
  use ExUnit.Case, async: true

  import Xandra.Protocol

  describe "frame_protocol_format/1" do
    test "returns the right \"format\"" do
      assert frame_protocol_format(Xandra.Protocol.V3) == :v4_or_less
      assert frame_protocol_format(Xandra.Protocol.V4) == :v4_or_less
      assert frame_protocol_format(Xandra.Protocol.V5) == :v5_or_more
    end
  end

  describe "decode_string/1" do
    test "decodes a string and rebinds variables" do
      encoded = <<3::16, "foo"::binary, "rest"::binary>>

      decode_string(contents <- encoded)

      assert contents == "foo"
      assert encoded == "rest"
    end

    test "raises if the size doesn't match" do
      encoded = <<3::16, "a"::binary>>

      assert_raise MatchError, fn ->
        decode_string(_ <- encoded)
        _ = encoded
      end
    end

    test "raises a compile-time error on malformed arguments" do
      message = "the right-hand side of <- must be a variable, got: :not_a_var"

      assert_raise ArgumentError, message, fn ->
        Code.eval_quoted(
          quote do
            decode_string(_ <- :not_a_var)
          end
        )
      end

      message = "the right-hand side of <- must be a variable, got: hello()"

      assert_raise ArgumentError, message, fn ->
        Code.eval_quoted(
          quote do
            decode_string(_ <- hello())
          end
        )
      end
    end
  end
end
