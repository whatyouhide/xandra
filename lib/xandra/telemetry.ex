defmodule Xandra.Telemetry do
  @moduledoc """
  Telemetry integration for event tracing, metrics, and logging.

  Xandra uses [telemetry](https://github.com/beam-telemetry/telemetry) for reporting
  metrics and events. Below we list all the possible events emitted by Xandra, alongside
  their measurements and metadata.

  Xandra emits telemetry events *since v0.15.0*.

  ### Xandra Connection

  * `[:xandra, :connection]` and `[:xandra, :disconnection]`

    * Measurements:

    * Metadata:
      * `:connection_name` - given name of the connection or `nil` if not set
      * `:host` - the host of the node the connection is connected to
      * `:port` - the port of the node the connection is connected to
      * `:reason` - reason of disconnection

  * `[:xandra, :prepare_query, :start]` and `[:xandra, :prepare_query, :stop]`

    * Measurements:
      * `:system_time` on `:start`
      * `:duration` on `:stop`

    * Metadata:
      * `:query` - the `t:Xandra.Prepared.t/0` query
      * `:connection_name` - given name of the connection or `nil` if not set
      * `:host` - the host of the node the connection is connected to
      * `:port` - the port of the node the connection is connected to
      * `:reason` - if error, reason

  * `[:xandra, :execute_query, :start]` and `[:xandra, :execute_query, :stop]`

    * Measurements:
      * `:system_time` on `:start`
      * `:duration` on `:stop`

    * Metadata:
      * `:query` - the `t:Xandra.Simple.t/0` or `t:Xandra.Batch.t/0` query
      * `:connection_name` - given name of the connection or `nil` if not set
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
      * `:host` - the host of the node the connection is connected to
      * `:port` - the port of the node the connection is connected to
      * `:current_keyspace` - the current keyspace of the connection, or `nil` if not set
      * `:query` - the query that caused the warning, of type `t:Xandra.Batch.t/0`,
        `t:Xandra.Prepared.t/0`, or `t:Xandra.Simple.t/0`

  ### Cluster events

  See the "Telemetry" section in the documentation for `Xandra.Cluster`.
  """
  @moduledoc since: "0.15.0"

  require Logger

  @doc """
  Attaches a handler that logs the given events:

  `[:xandra, :connection]` - logged at info level
  `[:xandra, :disconnection]` - logged at warn level
  `[:xandra, :prepared_cache, :hit]` and `[:xandra, :prepared_cache, :miss]` - logged at debug level
  `[:xandra, :prepare_query, :start]` and `[:xandra, :prepare_query, :stop]` - logged at debug level
  `[:xandra, :prepare_query, :exception]` - logged at exception level
  `[:xandra, :execute_query, :start]` and `[:xandra, :execute_query, :stop]` - logged at debug level
  `[:xandra, :execute_query, :exception]` - logged at exception level
  `[:xandra, :server_warnings]` - logged at warn level
  """
  def attach_default_handler() do
    events = [
      [:xandra, :connection],
      [:xandra, :disconnection],
      [:xandra, :prepared_cache, :hit],
      [:xandra, :prepared_cache, :miss],
      [:xandra, :prepare_query, :start],
      [:xandra, :prepare_query, :stop],
      [:xandra, :prepare_query, :exception],
      [:xandra, :execute_query, :start],
      [:xandra, :execute_query, :stop],
      [:xandra, :execute_query, :exception],
      [:xandra, :server_warnings]
    ]

    :telemetry.attach_many(
      "xandra-default-telemetry-handler",
      events,
      &__MODULE__.handle_event/4,
      :no_config
    )
  end

  @spec handle_event(nonempty_maybe_improper_list, any, any, :no_config) :: :ok
  def handle_event([:xandra | event], measurements, metadata, :no_config) do
    case event do
      [:connection] ->
        Logger.info("Connection established to #{metadata.host}:#{metadata.port}")

      [:disconnection] ->
        Logger.warn(
          "Disconnected from #{metadata.host}:#{metadata.port}. Reason: #{inspect(metadata.reason)}"
        )

      [:server_warnings] ->
        Logger.warn(
          "Received warning from #{metadata.host}:#{metadata.port}, " <>
            "warnings: #{inspect(measurements.warnings)}"
        )

      [:prepared_cache, status] ->
        Logger.debug("Prepared cache #{status} for query: #{inspect(measurements.query)}")

      [:prepare_query, :start] ->
        start = DateTime.from_unix!(measurements.system_time, :native)

        Logger.debug(
          "Started preparing query #{inspect(metadata.query)} at system_time: #{DateTime.to_string(start)}," <>
            " ref: #{inspect(metadata.telemetry_span_context)}"
        )

      [:prepare_query, :stop] ->
        duration = System.convert_time_unit(measurements.duration, :native, :millisecond)

        Logger.debug(
          "Finished preparing query  #{inspect(metadata.query)} in #{duration}ms" <>
            " ref: #{inspect(metadata.telemetry_span_context)}"
        )

      [:prepare_query, :exception] ->
        Logger.error(
          "An exception occcured while preparing  #{inspect(metadata.query)}, kind: #{metadata.kind}" <>
            "reason: #{inspect(metadata.reason)}, stacktrace: #{metadata.stacktrace}"
        )

      [:execute_query, :start] ->
        start = DateTime.from_unix!(measurements.system_time, :native)

        Logger.debug(
          "Started executing query  #{inspect(metadata.query)} at system_time: #{DateTime.to_string(start)}," <>
            " ref: #{inspect(metadata.telemetry_span_context)}"
        )

      [:execute_query, :stop] ->
        duration = System.convert_time_unit(measurements.duration, :native, :millisecond)

        Logger.debug(
          "Finished executing query  #{inspect(metadata.query)} in #{duration}ms" <>
            " ref: #{inspect(metadata.telemetry_span_context)}"
        )

      [:execute_query, :exception] ->
        Logger.error(
          "An exception occcured while executing  #{inspect(metadata.query)}, kind: #{metadata.kind}" <>
            "reason: #{inspect(metadata.reason)}, stacktrace: #{metadata.stacktrace}"
        )
    end
  end
end
