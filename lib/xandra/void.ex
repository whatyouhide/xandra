defmodule Xandra.Void do
  @moduledoc """
  A struct that represents an empty Cassandra result.

  This struct is returned as the result of queries such as `INSERT`, `UPDATE`, or
  `DELETE`.

  These are the public fields it contains:

    * `:tracing_id` - the tracing ID (as a UUID binary) if tracing was enabled,
      or `nil` if no tracing was enabled. See the "Tracing" section in `Xandra.execute/4`.

    * TODO

  """

  defstruct [:tracing_id, :custom_payload]

  @type t :: %__MODULE__{
          tracing_id: binary | nil,
          custom_payload: Xandra.custom_payload() | nil
        }
end
