defmodule Xandra.Telemetry do
  @moduledoc """
  Telemetry integration for event tracing, metrics, and logging.

  `Xandra` uses [telemetry](https://github.com/beam-telemetry/telemetry) for reporting
  metrics and events. Below we list all the possible events emitted by Xandra, alongside
  their measurements and metadata.

  ### Warnings

    * Event name: `[:xandra, :server_warnings]`

    * Measurements:
      * `:warnings` - A list of warnings where each warning is a string. It contains at least
        one element.

    * Metadata:
      * `:address` - the address of the node the connection is connected to
      * `:port` - the port of the node the connection is connected to
      * `:current_keyspace` - the current keyspace of the connection, or `nil` if not set
      * `:query` - the query that caused the warning, of type `t:Xandra.Batch.t/0`,
        `t:Xandra.Prepared.t/0`, or `t:Xandra.Simple.t/0`

  """
end
