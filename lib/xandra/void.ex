defmodule Xandra.Void do
  @moduledoc """
  A struct that represents an empty Cassandra result.

  This struct is returned as the result of queries such as `INSERT`, `UPDATE`, or
  `DELETE`.
  """

  defstruct [:tracing_id]

  @type t :: %__MODULE__{tracing_id: binary | nil}
end
