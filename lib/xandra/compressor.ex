defmodule Xandra.Compressor do
  @moduledoc """
  TODOTODO
  """

  @doc """
  TODO
  """
  @callback algorithm() :: :lz4 | :snappy

  @doc """
  TODO
  """
  @callback compress(binary) :: binary

  @doc """
  TODO
  """
  @callback decompress(binary) :: binary
end
