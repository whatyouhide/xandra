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

  """

  defstruct [:effect, :target, :options]

  @type t :: %__MODULE__{
          effect: String.t(),
          target: String.t(),
          options: map
        }
end
