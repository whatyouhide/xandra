defmodule Xandra.Compressor do
  @moduledoc """
  A behaviour to compress and decompress binary data.

  Modules implementing this behaviour can be used to compress and decompress
  data using one of the compression algorithms supported by Cassandra (as of
  now, lz4 and snappy).

  ## Example

  Let's imagine you implemented the snappy compression algorithm in your
  application:

      defmodule Snappy do
        def compress(binary), do: ...
        def decompress(binary), do: ...
      end

  You can then implement a module that implements the `Xandra.Compressor`
  behaviour and can be used to compress and decompress data flowing through the
  connection to Cassandra:

      defmodule SnappyXandraCompressor do
        @behaviour Xandra.Compressor

        def algorithm(), do: :snappy
        defdelegate compress(binary), to: Snappy
        defdelegate decompress(binary), to: Snappy
      end

  Now, this module can be passed as the value of the `:compressor` option to
  many functions in `Xandra`:

      Xandra.start_link(compressor: SnappyXandraCompressor)

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
  Deompresses the given binary according to the algorithm returned by
  `c:algorithm/0`.
  """
  @callback decompress(binary) :: binary
end
