defmodule Xandra.Protocol.CRCTest do
  use ExUnit.Case, async: true

  alias Xandra.Protocol.CRC

  describe "crc24/1" do
    test "calculates the CRC24 of the given number of bytes out of the given integer" do
      binary = <<113, 0, 2, 43, 55, 20>>
      expected_crc24 = 1_324_843

      assert CRC.crc24(binary) == expected_crc24
    end
  end

  describe "crc32/1" do
    test "calculates the CRC32 of the given number of bytes out of the given integer" do
      binary =
        <<133, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 10, 0, 98, 100, 55, 55, 54, 53, 52, 56, 98, 32,
          105, 110, 118, 97, 108, 105, 100, 44, 32, 117, 110, 114, 101, 99, 111, 118, 101, 114,
          97, 98, 108, 101, 32, 67, 82, 67, 32, 109, 105, 115, 109, 97, 116, 99, 104, 32, 100,
          101, 116, 101, 99, 116, 101, 100, 32, 105, 110, 32, 102, 114, 97, 109, 101, 32, 104,
          101, 97, 100, 101, 114, 46, 32, 82, 101, 97, 100, 32, 49, 55, 57, 50, 44, 32, 67, 111,
          109, 112, 117, 116, 101, 100, 32, 50, 51, 49, 50, 49, 57, 54>>

      expected_crc24 = 1_569_183_208

      assert CRC.crc32(binary) == expected_crc24
    end
  end
end
