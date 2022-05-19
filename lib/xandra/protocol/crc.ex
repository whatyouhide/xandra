defmodule Xandra.Protocol.CRC do
  @moduledoc false

  import Bitwise

  # Values taken from the Cassandra source code at the time of implementing this:
  # https://github.com/apache/cassandra/blob/7b927eaad3c42657951df688673598b2594514d4/src/java/org/apache/cassandra/net/Crc.java#L81
  @crc24_init 0x875060
  # https://github.com/apache/cassandra/blob/7b927eaad3c42657951df688673598b2594514d4/src/java/org/apache/cassandra/net/Crc.java#L95
  @crc24_poly 0x1974F0B

  # Initial value taken from the C* Python driver at the time of implementing this.
  # https://github.com/datastax/python-driver/blob/15d715f4e686032b02ce785eca1d176d2b25e32b/cassandra/segment.py#L26
  # CRC32_INITIAL = zlib.crc32(b("\xfa\x2d\x55\xca"))
  @crc32_init :erlang.crc32(<<0xFA, 0x2D, 0x55, 0xCA>>)

  @spec crc32(iodata()) :: non_neg_integer()
  def crc32(data) do
    :erlang.crc32(@crc32_init, data)
  end

  # Implementation of crc24/1 taken straight out of C*'s source code at the time of
  # writing this.
  # See: https://github.com/apache/cassandra/blob/7b927eaad3c42657951df688673598b2594514d4/src/java/org/apache/cassandra/net/Crc.java

  @spec crc24(binary()) :: non_neg_integer()
  def crc24(bytes) when is_binary(bytes) do
    crc24(@crc24_init, bytes)
  end

  defp crc24(crc, <<byte::integer-little, rest::binary>>) do
    crc = bxor(crc, byte <<< 16)

    crc =
      Enum.reduce(0..7, crc, fn _, crc ->
        crc = crc <<< 1

        if (crc &&& 0x1000000) != 0 do
          bxor(crc, @crc24_poly)
        else
          crc
        end
      end)

    crc24(crc, rest)
  end

  defp crc24(crc, <<>>) do
    crc
  end
end
