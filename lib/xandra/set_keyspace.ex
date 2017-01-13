defmodule Xandra.SetKeyspace do
  defstruct [:keyspace]

  @type t :: %__MODULE__{
    keyspace: String.t,
  }
end
