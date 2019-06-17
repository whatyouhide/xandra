defmodule Xandra.SetKeyspace do
  @moduledoc """
  A struct that represents the result of a `USE` query.

  These are the public fields of this struct:

    * `:keyspace` - the keyspace (as a binary) that was set through the executed
      `USE` query.

    * `:tracing_id` - the tracing ID (as a UUID binary) if tracing was enabled,
      or `nil` if no tracing was enabled. See the "Tracing" section in `Xandra.execute/4`.

  """

  defstruct [:keyspace, :tracing_id]

  @type t :: %__MODULE__{
          keyspace: String.t(),
          tracing_id: binary() | nil
        }
end
