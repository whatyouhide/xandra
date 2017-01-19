defmodule Xandra.FrameCompression do
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
  @callback uncompress(binary) :: binary
end
