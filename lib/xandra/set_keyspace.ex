defmodule Xandra.SetKeyspace do
  @moduledoc """
  A struct that represents the result of a `USE` query.

  This struct only has the `:keyspace` field, which contains the keyspace that
  was set through the executed `USE` query.
  """

  defstruct [:keyspace]

  @type t :: %__MODULE__{
          keyspace: String.t()
        }
end
