defmodule Xandra.Cluster.Token.Murmur3 do
  @moduledoc false

  # Implementation of the Cassandra-flavored MurmurHash3 (x64, 128-bit variant),
  # which is the hash function behind the Murmur3Partitioner used by both
  # Cassandra and ScyllaDB.
  #
  # Cassandra's implementation differs from the reference MurmurHash3 in that it
  # reads input bytes as *signed*, since it was written in Java. This matters for
  # tail bytes >= 0x80, which get sign-extended to 64 bits before being XORed in.
  # Body blocks are read as little-endian 64-bit integers, where signedness makes
  # no difference modulo 2^64.

  import Bitwise

  @mask64 0xFFFFFFFFFFFFFFFF

  @c1 0x87C37B91114253D5
  @c2 0x4CF5AD432745937F

  # Returns the hash as a *signed* 64-bit integer (the "h1" half of the 128-bit
  # hash), which is what Cassandra and ScyllaDB use as the token value.
  @spec hash(binary()) :: integer()
  def hash(data) when is_binary(data) do
    {h1, h2, tail} = hash_blocks(data, 0, 0)

    {k1, k2} = tail_blocks(tail)

    h2 =
      if byte_size(tail) > 8 do
        k2 = k2 |> mul64(@c2) |> rotl64(33) |> mul64(@c1)
        bxor(h2, k2)
      else
        h2
      end

    h1 =
      if byte_size(tail) > 0 do
        k1 = k1 |> mul64(@c1) |> rotl64(31) |> mul64(@c2)
        bxor(h1, k1)
      else
        h1
      end

    # Finalization.
    h1 = bxor(h1, byte_size(data))
    h2 = bxor(h2, byte_size(data))

    h1 = add64(h1, h2)
    h2 = add64(h2, h1)

    h1 = fmix64(h1)
    h2 = fmix64(h2)

    h1 = add64(h1, h2)

    to_signed64(h1)
  end

  defp hash_blocks(<<k1::64-little, k2::64-little, rest::binary>>, h1, h2) do
    k1 = k1 |> mul64(@c1) |> rotl64(31) |> mul64(@c2)
    h1 = bxor(h1, k1)
    h1 = h1 |> rotl64(27) |> add64(h2)
    h1 = h1 |> mul64(5) |> add64(0x52DCE729)

    k2 = k2 |> mul64(@c2) |> rotl64(33) |> mul64(@c1)
    h2 = bxor(h2, k2)
    h2 = h2 |> rotl64(31) |> add64(h1)
    h2 = h2 |> mul64(5) |> add64(0x38495AB5)

    hash_blocks(rest, h1, h2)
  end

  defp hash_blocks(tail, h1, h2) when byte_size(tail) < 16 do
    {h1, h2, tail}
  end

  # Builds the (k1, k2) pair from the tail (< 16 bytes), reading each byte as a
  # *signed* byte sign-extended to 64 bits, shifted into position.
  defp tail_blocks(tail) do
    tail
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.reduce({0, 0}, fn {byte, index}, {k1, k2} ->
      signed_byte = band(byte - band(byte, 0x80) * 2, @mask64)

      if index < 8 do
        {bxor(k1, band(signed_byte <<< (index * 8), @mask64)), k2}
      else
        {k1, bxor(k2, band(signed_byte <<< ((index - 8) * 8), @mask64))}
      end
    end)
  end

  defp fmix64(k) do
    k = bxor(k, k >>> 33)
    k = mul64(k, 0xFF51AFD7ED558CCD)
    k = bxor(k, k >>> 33)
    k = mul64(k, 0xC4CEB9FE1A85EC53)
    bxor(k, k >>> 33)
  end

  defp mul64(x, y), do: band(x * y, @mask64)

  defp add64(x, y), do: band(x + y, @mask64)

  defp rotl64(x, r), do: band(x <<< r, @mask64) ||| x >>> (64 - r)

  defp to_signed64(x) when x > 0x7FFFFFFFFFFFFFFF, do: x - 0x10000000000000000
  defp to_signed64(x), do: x
end
