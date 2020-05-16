defmodule Xandra.Void do
  @moduledoc """
  A struct that represents an empty Cassandra result.

  This struct is returned as the result of queries such as `INSERT`, `UPDATE`, or
  `DELETE`.

  These are the public fields it contains:

    * `:tracing_id` - the tracing ID (as a UUID binary) if tracing was enabled,
      or `nil` if no tracing was enabled. See the "Tracing" section in `Xandra.execute/4`.

    * `:custom_payload` - the custom payload sent along with the response (only
      protocol version 4). This is used by Azure Cosmos DB to inform about the
      RequestCharge, for example. See the "Custom Payload" section in `Xandra.execute/4`.

  """

  defstruct [:tracing_id, :custom_payload]

  @type t :: %__MODULE__{tracing_id: binary | nil, custom_payload: [{String.t(), binary}] | nil}
end
