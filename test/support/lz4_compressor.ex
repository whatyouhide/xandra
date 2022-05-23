defmodule Xandra.TestHelper.LZ4Compressor do
  @behaviour Xandra.Compressor

  import ExUnit.Assertions

  @impl true
  def algorithm(), do: :lz4

  @impl true
  def compress(body) do
    # 32-bit big-endian integer with the size of the uncompressed body followed by
    # the compressed body.
    [<<IO.iodata_length(body)::4-unit(8)-integer>>, NimbleLZ4.compress(body)]
  end

  @impl true
  def decompress(<<uncompressed_size::4-unit(8)-integer, compressed_body::binary>>) do
    assert {:ok, body} = NimbleLZ4.decompress(compressed_body, uncompressed_size)
    body
  end
end
