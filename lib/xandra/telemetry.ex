defmodule Xandra.Telemetry do
  @moduledoc """
  Telemetry integration for event tracing, metrics, and logging.

  Xandra uses [telemetry](https://github.com/beam-telemetry/telemetry) for reporting
  metrics and events. Below we list all the possible events emitted by Xandra, alongside
  their measurements and metadata.

  Xandra emits telemetry events *since v0.15.0*.

  ## Events

  For a comprehensive list of the events that Xandra emits, see the
  [*Telemetry Events*](telemetry-events.html) page in the guides.
  """

  @moduledoc since: "0.15.0"

  alias Xandra.Cluster.Host

  require Logger

  @doc """
  Attaches a handler that **logs** Telemetry events.

  This handler is useful when you want to see what's going on in Xandra without having to write a
  Telemetry handler to handle all the events.

  These are the events that get logged. This list might change in the future.

  | **Event**                                                      | **Level** |
  | -------------------------------------------------------------- | --------- |
  | `[:xandra, :connected]`                                        | info      |
  | `[:xandra, :disconnected]`                                     | warn      |
  | `[:xandra, :failed_to_connect]`                                | warn      |
  | `[:xandra, :prepared_cache, :hit]`                             | debug     |
  | `[:xandra, :prepared_cache, :miss]`                            | debug     |
  | `[:xandra, :prepare_query, :start]`                            | debug     |
  | `[:xandra, :prepare_query, :stop]`                             | debug     |
  | `[:xandra, :prepare_query, :exception]`                        | error     |
  | `[:xandra, :execute_query, :start]`                            | debug     |
  | `[:xandra, :execute_query, :stop]`                             | debug     |
  | `[:xandra, :execute_query, :exception]`                        | error     |
  | `[:xandra, :server_warnings]`                                  | warn      |
  | `[:xandra, :cluster, :change_event]`                           | debug     |
  | `[:xandra, :cluster, :control_connection, :connected]`         | debug     |
  | `[:xandra, :cluster, :control_connection, :disconnected]`      | debug     |
  | `[:xandra, :cluster, :control_connection, :failed_to_connect]` | warn      |
  | `[:xandra, :cluster, :pool, :started]`                         | debug     |
  | `[:xandra, :cluster, :pool, :restarted]`                       | debug     |
  | `[:xandra, :cluster, :discovered_peers]`                       | debug     |

  Events have the following logger metadata:

    * `:xandra_address` - the address of the node the connection is connected to
    * `:xandra_port` - the port of the node the connection is connected to
    * `:xandra_protocol_module` - the protocol module for the Cassandra native protocol

  """
  @doc since: "0.15.0"
  @spec attach_default_handler() :: :ok
  def attach_default_handler do
    events = [
      [:xandra, :connected],
      [:xandra, :disconnected],
      [:xandra, :failed_to_connect],
      [:xandra, :prepared_cache, :hit],
      [:xandra, :prepared_cache, :miss],
      [:xandra, :prepare_query, :stop],
      [:xandra, :execute_query, :stop],
      [:xandra, :server_warnings],
      [:xandra, :cluster, :change_event],
      [:xandra, :cluster, :control_connection, :connected],
      [:xandra, :cluster, :control_connection, :disconnected],
      [:xandra, :cluster, :control_connection, :failed_to_connect],
      [:xandra, :cluster, :pool, :started],
      [:xandra, :cluster, :pool, :restarted],
      [:xandra, :cluster, :pool, :stopped],
      [:xandra, :cluster, :discovered_peers]
    ]

    :telemetry.attach_many(
      "xandra-default-telemetry-handler",
      events,
      &__MODULE__.handle_event/4,
      :no_config
    )

    :ok
  end

  # Used for debugging Xandra itself.
  @doc false
  @spec attach_debug_handler() :: :ok
  def attach_debug_handler do
    events = [
      [:xandra, :debug, :received_frame],
      [:xandra, :debug, :sent_frame],
      [:xandra, :connected]
    ]

    :telemetry.attach_many(
      "xandra-debug-telemetry-handler",
      events,
      &__MODULE__.handle_debug_event/4,
      :no_config
    )
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
    logger_meta =
      case Map.fetch(metadata, :host) do
        {:ok, %Host{address: address, port: port}} ->
          [xandra_address: address_to_string(address), xandra_port: port]

        :error ->
          []
      end

    case event do
      [:change_event] ->
        Logger.debug("Received change event: #{inspect(metadata.event_type)}", logger_meta)

      [:control_connection, :connected] ->
        Logger.debug("Control connection established", logger_meta)

      [:control_connection, :disconnected] ->
        Logger.debug("Control connection disconnected", logger_meta)

      [:control_connection, :failed_to_connect] ->
        Logger.warning("Control connection failed to connect", logger_meta)

      [:pool, :started] ->
        Logger.debug("Pool started", logger_meta)

      [:pool, :restarted] ->
        Logger.debug("Pool restarted", logger_meta)

      [:pool, :stopped] ->
        Logger.debug("Pool stopped", logger_meta)

      [:discovered_peers] ->
        Logger.debug("Discovered peers: #{inspect(measurements.peers)}", logger_meta)
    end
  end

  def handle_event([:xandra | event], measurements, metadata, :no_config) do
    %{address: address, port: port} = metadata
    logger_meta = [xandra_address: address_to_string(address), xandra_port: port]

    case event do
      [:connected] ->
        Logger.info("Connection established", logger_meta)

      [:disconnected] ->
        Logger.warning("Disconnected with reason: #{inspect(metadata.reason)}", logger_meta)

      [:failed_to_connect] ->
        Logger.warning("Failed to connect with reason: #{inspect(metadata.reason)}", logger_meta)

      [:server_warnings] ->
        Logger.warning("Received warnings: #{inspect(measurements.warnings)}", logger_meta)

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

  @doc false
  def handle_debug_event(event, measurements, metadata, config)

  def handle_debug_event([:xandra, :debug, :received_frame], _measurements, metadata, :no_config) do
    Logger.debug("Received frame #{metadata.frame_type}", [])
  end

  def handle_debug_event([:xandra, :debug, :sent_frame], measurements, metadata, :no_config) do
    message =
      if metadata.frame_type == :STARTUP do
        "Sent frame STARTUP with protocol #{inspect(measurements.protocol_module)} " <>
          "and requested options: #{inspect(measurements.requested_options)}"
      else
        "Sent frame #{metadata.frame_type}"
      end

    Logger.debug(message)
  end

  def handle_debug_event(
        [:xandra, :debug, :downgrading_protocol],
        _measurements,
        metadata,
        :no_config
      ) do
    Logger.debug(
      "Could not use protocol #{inspect(metadata.failed_version)}, " <>
        "downgrading to #{inspect(metadata.new_version)}",
      xandra_address: address_to_string(metadata.address),
      xandra_port: metadata.port
    )
  end

  def handle_debug_event([:xandra, :connected], _measurements, metadata, :no_config) do
    logger_meta = [
      xandra_address: address_to_string(metadata.address),
      xandra_port: metadata.port
    ]

    Logger.debug("Connected using protocol #{inspect(metadata.protocol_module)}", logger_meta)
    Logger.debug("Supported options: #{inspect(metadata.supported_options)}", logger_meta)
  end

  defp address_to_string(ip) when is_tuple(ip), do: ip |> :inet.ntoa() |> to_string()
  defp address_to_string(other), do: to_string(other)
end
