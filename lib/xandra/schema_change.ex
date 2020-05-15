defmodule Xandra.SchemaChange do
  @moduledoc """
  A struct that represents the result of a query that modifies the schema.

  This struct has the following fields:

    * `:effect` - the type of change involved. It's one of `"CREATED"`,
      `"UPDATED"`, or `"DROPPED"`.

    * `:target` - what has been modified. It's one of `"KEYSPACE"`, `"TABLE"`,
      or `"TYPE"`.

    * `:options` - a map of options that depends on the value of `:target`:
      * if target is `"KEYSPACE"`, the map will have the form
        `%{keyspace: keyspace}`
      * if the target is `"TABLE"` or `"TYPE"`, the map will have the form
        `%{keyspace: keyspace, subject: subject}` where `keyspace` is the
        keyspace where the change happened and `subject` is the name of what
        changed (so the name of the changed table or type)

    * `:tracing_id` - the tracing ID (as a UUID binary) if tracing was enabled,
      or `nil` if no tracing was enabled. See the "Tracing" section in `Xandra.execute/4`.

    * `:custom_payload` - the custom payload sent along with the response (only
      protocol version 4). This is used by Azure Cosmos DB to inform about the
      RequestCharge, for example. See the "Custom Payload" section in `Xandra.execute/4`.

  """

  defstruct [:effect, :target, :options, :tracing_id, :custom_payload]

  @type t :: %__MODULE__{
          effect: String.t(),
          target: String.t(),
          options: map,
          tracing_id: binary | nil,
          custom_payload: list({String.t(), binary()}) | nil
        }
end
