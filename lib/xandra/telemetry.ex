defmodule Xandra.Telemetry do
  @moduledoc """
  Telemetry integration for event tracing, metrics, and logging.

  Xandra uses [telemetry](https://github.com/beam-telemetry/telemetry) for reporting
  metrics and events. Below we list all the possible events emitted by Xandra, alongside
  their measurements and metadata.

  Xandra emits telemetry events *since v0.15.0*.

  ## Events

  Here is a comprehensive list of the Telemetry events that Xandra emits.

  ### Connection events

    * `[:xandra, :connected]` — executed when a connection connects to its Cassandra node.
      * **Measurements**: *none*.
      * **Metadata**:
        * `:connection_name` - given name of the connection or `nil` if not set
        * `:address` - the address of the node the connection is connected to
        * `:port` - the port of the node the connection is connected to

    * `[:xandra, :disconnected]` — executed when a connection disconnects from its Cassandra node.
      * **Measurements**: *none*.
      * **Metadata**:
        * `:connection_name` - given name of the connection or `nil` if not set
        * `:address` - the address of the node the connection is connected to
        * `:port` - the port of the node the connection is connected to
        * `:reason` - the reason for the disconnection (usually a `DBConnection.ConnectionError`)

  ### Query events

  The `[:xandra, :prepare_query, ...]` and `[:xandra, :execute_query, ...]` events are
  Telemetry **spans**. See
  [`telemetry:span/3`](https://hexdocs.pm/telemetry/telemetry.html#span/3). All the time
  measurements are in *native* time unit, so you need to use `System.convert_time_unit/3`
  to convert to the desired time unit.

    * `[:xandra, :prepare_query, :start]` — executed before a query is prepared.
      * Measurements:
        * `:system_time` (in `:native` time units)
        * `:monotonic_time` (in `:native` time units)
      * Metadata:
        * `:query` (`t:Xandra.Prepared.t/0`) - the query being prepared
        * `:connection_name` - given name of the connection or `nil` if not set
        * `:address` - the address of the node the connection is connected to
        * `:port` - the port of the node the connection is connected to
        * `:extra_metadata` - extra metadata provided by `:telemetry_metadata` option

    * `[:xandra, :prepare_query, :stop]` — executed after a query was prepared.
      * Measurements:
        * `:duration` (in `:native` time units)
        * `:monotonic_time` (in `:native` time units)
      * Metadata:
        * `:query` (`t:Xandra.Prepared.t/0`) - the query being prepared
        * `:connection_name` - given name of the connection or `nil` if not set
        * `:address` - the address of the node the connection is connected to
        * `:port` - the port of the node the connection is connected to
        * `:reason` - if error, reason
        * `:extra_metadata` - extra metadata provided by `:telemetry_metadata` option

    * `[:xandra, :prepare_query, :exception]` — executed if there was an exception
      when preparing a query.
      * Measurements:
        * `:duration` (in `:native` time units)
        * `:monotonic_time` (in `:native` time units)
      * Metadata:
        * `:query` (`t:Xandra.Prepared.t/0`) - the query being prepared
        * `:connection_name` - given name of the connection or `nil` if not set
        * `:address` - the address of the node the connection is connected to
        * `:port` - the port of the node the connection is connected to
        * `:reason` - if error, reason
        * `:kind` - kind on `:exception`
        * `:stacktrace` - stacktrace on `:exception`
        * `:extra_metadata` - extra metadata provided by `:telemetry_metadata` option

    * `[:xandra, :execute_query, :start]` — executed before a query is executed.
      * Measurements:
        * `:system_time` (in `:native` time units)
        * `:monotonic_time` (in `:native` time units)
      * Metadata:
        * `:query` (`t:Xandra.Simple.t/0`, `t:Xandra.Batch.t/0`, or `t:Xandra.Prepared.t/0`) —
          the query being executed
        * `:connection_name` - given name of the connection or `nil` if not set
        * `:address` - the address of the node the connection is connected to
        * `:port` - the port of the node the connection is connected to
        * `:extra_metadata` - extra metadata provided by `:telemetry_metadata` option

    * `[:xandra, :execute_query, :stop]` — executed after a query was executed.
      * Measurements:
        * `:duration` (in `:native` time units)
        * `:monotonic_time` (in `:native` time units)
      * Metadata:
        * `:query` (`t:Xandra.Simple.t/0`, `t:Xandra.Batch.t/0`, or `t:Xandra.Prepared.t/0`) —
          the query being executed
        * `:connection_name` - given name of the connection or `nil` if not set
        * `:address` - the address of the node the connection is connected to
        * `:port` - the port of the node the connection is connected to
        * `:reason` - if error, reason
        * `:extra_metadata` - extra metadata provided by `:telemetry_metadata` option

    * `[:xandra, :execute_query, :exception]` — executed if there was an exception
      when executing a query.
      * Measurements:
        * `:duration` (in `:native` time units)
        * `:monotonic_time` (in `:native` time units)
      * Metadata:
        * `:query` (`t:Xandra.Simple.t/0`, `t:Xandra.Batch.t/0`, or `t:Xandra.Prepared.t/0`) —
          the query being executed
        * `:connection_name` - given name of the connection or `nil` if not set
        * `:address` - the address of the node the connection is connected to
        * `:port` - the port of the node the connection is connected to
        * `:reason` - if error, reason
        * `:kind` - kind on `:exception`
        * `:stacktrace` - stacktrace on `:exception`
        * `:extra_metadata` - extra metadata provided by `:telemetry_metadata` option

    * `[:xandra, :prepared_cache, :hit]` and `[:xandra, :prepared_cache, :miss]` — executed
      when a query is executed and the prepared cache is checked.

      * Measurements:
        * `:query` (`t:Xandra.Prepared.t/0`) - the query being looked up in the cache
        * `:connection_name` - given name of the connection or `nil` if not set
        * `:address` - the address of the node the connection is connected to
        * `:port` - the port of the node the connection is connected to
        * `:extra_metadata` - extra metadata provided by `:telemetry_metadata` option

  ### Warnings

    * `[:xandra, :server_warnings]`
      * Measurements:
        * `:warnings` - A list of warnings where each warning is a string. It contains at least
          one element.
      * Metadata:
        * `:address` - the address of the node the connection is connected to
        * `:port` - the port of the node the connection is connected to
        * `:current_keyspace` - the current keyspace of the connection, or `nil` if not set
        * `:query` - the query that caused the warning, of type `t:Xandra.Batch.t/0`,
          `t:Xandra.Prepared.t/0`, or `t:Xandra.Simple.t/0`

  ### Cluster events

  See the "Telemetry" section in the documentation for `Xandra.Cluster`.
  """
  @moduledoc since: "0.15.0"

  alias Xandra.Cluster.Host

  require Logger

  @doc """
  Attaches a handler that **logs** Telemetry events.

  This handler is useful when you want to see what's going on in Xandra without having to write a
  Telemetry handler to handle all the events.

  These are the events that get logged. This list might change in the future.

  | **Event**                                                 | **Level** |
  | --------------------------------------------------------- | --------- |
  | `[:xandra, :connected]`                                   | info      |
  | `[:xandra, :disconnected]`                                | warn      |
  | `[:xandra, :prepared_cache, :hit]`                        | debug     |
  | `[:xandra, :prepared_cache, :miss]`                       | debug     |
  | `[:xandra, :prepare_query, :start]`                       | debug     |
  | `[:xandra, :prepare_query, :stop]`                        | debug     |
  | `[:xandra, :prepare_query, :exception]`                   | error     |
  | `[:xandra, :execute_query, :start]`                       | debug     |
  | `[:xandra, :execute_query, :stop]`                        | debug     |
  | `[:xandra, :execute_query, :exception]`                   | error     |
  | `[:xandra, :server_warnings]`                             | warn      |
  | `[:xandra, :cluster, :change_event]`                      | debug     |
  | `[:xandra, :cluster, :control_connection, :connected]`    | debug     |
  | `[:xandra, :cluster, :control_connection, :disconnected]` | debug     |

  Events have the following logger metadata:

    * `:xandra_address` - the address of the node the connection is connected to
    * `:xandra_port` - the port of the node the connection is connected to
    * `:xandra_protocol_module` - the protocol module for the Cassandra native protocol

  """
  @spec attach_default_handler() :: :ok
  def attach_default_handler do
    events = [
      [:xandra, :connected],
      [:xandra, :disconnected],
      [:xandra, :prepared_cache, :hit],
      [:xandra, :prepared_cache, :miss],
      [:xandra, :prepare_query, :stop],
      [:xandra, :execute_query, :stop],
      [:xandra, :server_warnings],
      [:xandra, :cluster, :change_event],
      [:xandra, :cluster, :control_connection, :connected],
      [:xandra, :cluster, :control_connection, :disconnected]
    ]

    :telemetry.attach_many(
      "xandra-default-telemetry-handler",
      events,
      &__MODULE__.handle_event/4,
      :no_config
    )

    :ok
  end

  @doc false
  @spec handle_event(
          :telemetry.event_name(),
          :telemetry.event_measurements(),
          :telemetry.event_metadata(),
          :no_config
        ) :: :ok
  def handle_event(event, measurements, metadata, config)

  def handle_event([:xandra, :cluster | event], measurements, metadata, :no_config) do
    %Host{address: address, port: port} = metadata.host
    logger_meta = [xandra_address: address, xandra_port: port]

    case event do
      [:change_event] ->
        Logger.debug("Received change event: #{inspect(measurements.event)}", logger_meta)

      [:control_connection, :connected] ->
        Logger.debug("Control connection established", logger_meta)

      [:control_connection, :disconnected] ->
        Logger.debug("Control connection disconnected", logger_meta)
    end
  end

  def handle_event([:xandra | event], measurements, metadata, :no_config) do
    %{address: address, port: port} = metadata
    logger_meta = [xandra_address: address, xandra_port: port]

    case event do
      [:connected] ->
        Logger.info("Connection established", logger_meta)

      [:disconnected] ->
        Logger.warn("Disconnected with reason: #{inspect(metadata.reason)}", logger_meta)

      [:server_warnings] ->
        Logger.warn("Received warnings: #{inspect(measurements.warnings)}", logger_meta)

      [:prepared_cache, status] when status in [:hit, :miss] ->
        query = inspect(metadata.query)
        Logger.debug("Prepared cache #{status} for query: #{query}", logger_meta)

      [:prepare_query, :stop] ->
        duration = System.convert_time_unit(measurements.duration, :native, :millisecond)
        Logger.debug("Prepared query in #{duration}ms: #{inspect(metadata.query)}", logger_meta)

      [:execute_query, :stop] ->
        duration = System.convert_time_unit(measurements.duration, :native, :millisecond)
        Logger.debug("Executed query in #{duration}ms: #{inspect(metadata.query)}", logger_meta)
    end
  end
end
