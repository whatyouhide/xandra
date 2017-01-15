defmodule Xandra.Void do
  @moduledoc """
  A struct that represents an empty Cassandra result.

  This struct is returned as the result of queries such as `INSERT`, `UPDATE`, or
  `DELETE`.
  """

  defstruct []

  @type t :: %__MODULE__{}
end
