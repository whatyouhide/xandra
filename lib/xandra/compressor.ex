defmodule Xandra.Compressor do
  @moduledoc """
  A behaviour to compress and decompress binary data.

  Modules implementing this behaviour can be used to compress and decompress
  data using one of the compression algorithms supported by Cassandra (see below).

  ## Supported algorithms and implementations

  Native protocol versions v4 and earlier support two compression algorithms:
  [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) and
  [Snappy](https://en.wikipedia.org/wiki/Snappy_(compression)).

  Native protocol versions v5 and later only support LZ4.

  ### LZ4

  If you implement a compressor module for the LZ4 algorithm, then:

    * The `c:compress/1` callback **must** return the compressed payload
      *preceded* by a 32-bit big-endian unsigned integer representing the
      length (in bytes) of the **uncompressed body**.

    * Xandra will call the `c:decompress/1` callback with the compressed
      payload, also preceded by the uncompressed body size (in bytes) as
      a 32-bit big-endian unsigned integer.

  That is, the result of compression when using the LZ4 algorithm must look like
  this:

      <<uncompressed_payload_length::32-big-unsigned, compressed_payload::binary>>

  Snappy is self-sufficient so it doesn't need the prepended uncompressed-payload
  size.

  ## Example

  Let's imagine you implemented the LZ compression algorithm in your application:

      defmodule MyApp.LZ4 do
        def compress(binary), do: # ...
        def decompress(binary, uncompressed_size), do: # ...
      end

  You can then implement a module that implements the `Xandra.Compressor`
  behaviour and can be used to compress and decompress data flowing through the
  connection to Cassandra:

      defmodule LZ4XandraCompressor do
        @behaviour Xandra.Compressor

        @impl true
        def algorithm(), do: :lz4

        @impl true
        def compress(body) do
          [<<IO.iodata_length(body)::4-unit(8)-integer>>, MyApp.LZ4.compress(body)]
        end

        @impl true
        def decompress(<<uncompressed_size::4-unit(8)-integer, compressed_body::binary>>) do
          MyApp.LZ4.decompress(compressed_body, uncompressed_size)
        end
      end

  Now, this module can be passed as the value of the `:compressor` option to
  many functions in `Xandra`:

      Xandra.start_link(compressor: LZ4XandraCompressor)

  For more information on compression, see the "Compression" section in the
  documentation for `Xandra.`
  """

  @doc """
  Specifies which algorithm this module will use to compress and decompress
  data.
  """
  @callback algorithm() :: :lz4 | :snappy

  @doc """
  Compresses the given iodata according to the algorithm returned by
  `c:algorithm/0`.
  """
  @callback compress(iodata) :: iodata

  @doc """
  Decompresses the given binary according to the algorithm returned by
  `c:algorithm/0`.
  """
  @callback decompress(binary) :: binary
end
