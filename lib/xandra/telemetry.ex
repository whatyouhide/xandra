defmodule Xandra.Telemetry do
  @moduledoc """
  Telemetry integration for event tracing, metrics, and logging.

  `Xandra` uses [telemetry](https://github.com/beam-telemetry/telemetry) for reporting
  metrics and events. Below we list all the possible events emitted by Xandra, alongside
  their measurements and metadata.

  ### Xandra Connection

  * `[:xandra, :connection]` and `[:xandra, :disconnection]`

    * Measurements:

    * Metadata:
      * `:connection_name` - given name of the connection or `nil`
      * `:host` - the host of the node the connection is connected to
      * `:port` - the port of the node the connection is connected to
      * `:reason` - reason of disconnection

  * `[:xandra, :prepare_query, :start]` and `[:xandra, :prepare_query, :stop]`

    * Measurements:
      * `:system_time` on `:start`
      * `:duration` on `:stop`

    * Metadata:
      * `:query` - the `t:Xandra.Prepared.t/0` query
      * `:connection_name` - given name of the connection or `nil`
      * `:host` - the host of the node the connection is connected to
      * `:port` - the port of the node the connection is connected to
      * `:reason` - if error, reason

  * `[:xandra, :execute_query, :start]` and `[:xandra, :execute_query, :stop]`

    * Measurements:
      * `:system_time` on `:start`
      * `:duration` on `:stop`

    * Metadata:
      * `:query` - the `t:Xandra.Simple.t/0` or `t:Xandra.Batch.t/0` query
      * `:connection_name` - given name of the connection or `nil`
      * `:host` - the host of the node the connection is connected to
      * `:port` - the port of the node the connection is connected to
      * `:reason` - if error, reason

  * `[:xandra, :prepared_cache, :hit]` and `[:xandra, :prepared_cache, :miss]`

    * Measurements:
      * `:query` - the `t:Xandra.Prepared.t/0` query

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

  require Logger

  def attach_default_handler() do
    events = [
      [:xandra, :connection],
      [:xandra, :disconnection],
      [:xandra, :prepared_cache, :hit],
      [:xandra, :prepared_cache, :miss],
      [:xandra, :prepare_query, :start],
      [:xandra, :prepare_query, :stop],
      [:xandra, :execute_query, :start],
      [:xandra, :execute_query, :stop]
    ]

    :telemetry.attach_many(
      "xandra-default-telemetry-handler",
      events,
      &__MODULE__.handle_event/4,
      :no_config
    )
  end

  def handle_event([:xandra | event], measurements, metadata, :no_config) do
    :ok
  end
end
