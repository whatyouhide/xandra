defmodule Xandra.Void do
  @moduledoc """
  A struct that represents an empty Cassandra result.

  This struct is returned as the result of queries such as `INSERT`, `UPDATE`, or
  `DELETE`. See [`%Xandra.Void{}`](`__struct__/0`) for information about the fields.
  """

  @doc """
  The struct for "void" results.

  These are the public fields it contains:

    * `:tracing_id` - the tracing ID (as a UUID binary) if tracing was enabled,
      or `nil` if no tracing was enabled. See the "Tracing" section in `Xandra.execute/4`.

    * `:custom_payload` - the *custom payload* sent by the server, if present.
      If the server doesn't send a custom payload, this field is `nil`. Otherwise,
      it's of type `t:Xandra.custom_payload/0`. See the "Custom payloads" section
      in the documentation for the `Xandra` module.

  """
  defstruct [:tracing_id, :custom_payload]

  @typedoc """
  The type for a "void" result.
  """
  @type t :: %__MODULE__{
          tracing_id: binary | nil,
          custom_payload: Xandra.custom_payload() | nil
        }
end
