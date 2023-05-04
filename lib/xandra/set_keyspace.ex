defmodule Xandra.SetKeyspace do
  @moduledoc """
  A struct that represents the result of a `USE` query.

  These are the public fields of this struct:

    * `:keyspace` - the keyspace (as a binary) that was set through the executed
      `USE` query.

    * `:tracing_id` - the tracing ID (as a UUID binary) if tracing was enabled,
      or `nil` if no tracing was enabled. See the "Tracing" section in `Xandra.execute/4`.

    * `:custom_payload` - the *custom payload* sent by the server, if present.
      If the server doesn't send a custom payload, this field is `nil`. Otherwise,
      it's of type `t:Xandra.custom_payload/0`. See the "Custom payloads" section
      in the documentation for the `Xandra` module.

  """

  defstruct [:keyspace, :tracing_id, :custom_payload]

  @typedoc """
  The type for a "set keyspace" result.
  """
  @type t :: %__MODULE__{
          keyspace: String.t(),
          tracing_id: binary() | nil,
          custom_payload: Xandra.custom_payload() | nil
        }
end
