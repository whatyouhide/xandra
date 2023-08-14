defmodule Xandra.Cluster.ControlConnectionTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Xandra.Frame
  alias Xandra.TestHelper
  alias Xandra.Transport

  alias Xandra.Cluster.{
    ControlConnection,
    Host,
    StatusChange,
    TopologyChange
  }

  @protocol_version XandraTest.IntegrationCase.protocol_version()
  @port String.to_integer(System.get_env("CASSANDRA_PORT", "9052"))

  setup do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)

    # Base start options for the control connection.
    start_options = [
      cluster_pid: mirror,
      cluster_name: nil,
      refresh_topology_interval: 60_000,
      autodiscovered_nodes_port: @port,
      connection_options: [protocol_version: @protocol_version],
      contact_node: {~c"127.0.0.1", @port}
    ]

    %{mirror_ref: mirror_ref, mirror: mirror, start_options: start_options}
  end

  test "reports data upon successful connection",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    start_control_connection!(start_options)
    assert_receive {^mirror_ref, {:discovered_hosts, [local_peer]}}
    assert %Host{address: {127, 0, 0, 1}, data_center: "datacenter1", rack: "rack1"} = local_peer
  end

  test "fails to start if it can't connect to the contact point node",
       %{start_options: start_options} do
    telemetry_event = [:xandra, :cluster, :control_connection, :failed_to_connect]
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [telemetry_event])

    options = Keyword.put(start_options, :contact_node, {~c"127.0.0.1", 9039})
    assert {:error, _} = start_supervised({ControlConnection, options})

    assert_receive {^telemetry_event, ^telemetry_ref, %{}, metadata}
    assert %{reason: :econnrefused, host: %Host{address: ~c"127.0.0.1", port: 9039}} = metadata
  end

  test "dies if the connected node closes its socket",
       %{mirror_ref: mirror_ref, mirror: mirror, start_options: start_options} do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:xandra, :cluster, :control_connection, :disconnected],
        [:xandra, :cluster, :control_connection, :connected]
      ])

    assert {:ok, ctrl_conn} = start_supervised({ControlConnection, start_options})
    monitor_ref = Process.monitor(ctrl_conn)

    assert_receive {^mirror_ref, {:discovered_hosts, _peers}}

    assert_receive {[:xandra, :cluster, :control_connection, :connected], ^telemetry_ref, %{},
                    %{
                      cluster_name: nil,
                      cluster_pid: ^mirror,
                      host: %Host{address: {127, 0, 0, 1}}
                    }}

    # Manually simulate closing the socket
    send(ctrl_conn, {:tcp_closed, :sys.get_state(ctrl_conn).transport.socket})

    assert_receive {:DOWN, ^monitor_ref, _, _, {:shutdown, :closed}}

    assert_receive {[:xandra, :cluster, :control_connection, :disconnected], ^telemetry_ref, %{},
                    %{
                      cluster_name: nil,
                      cluster_pid: ^mirror,
                      host: %Host{address: {127, 0, 0, 1}},
                      reason: :closed
                    }}

    # Assert that it eventually reconnects
    assert_receive {[:xandra, :cluster, :control_connection, :connected], ^telemetry_ref, %{},
                    %{}}
  end

  test "dies if the connected node's socket errors out", %{start_options: start_options} do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:xandra, :cluster, :control_connection, :connected],
        [:xandra, :cluster, :control_connection, :disconnected]
      ])

    assert {:ok, ctrl_conn} = start_supervised({ControlConnection, start_options})
    monitor_ref = Process.monitor(ctrl_conn)

    assert_receive {[:xandra, :cluster, :control_connection, :connected], ^telemetry_ref, _, _}

    send(ctrl_conn, {:tcp_error, :sys.get_state(ctrl_conn).transport.socket, :econnreset})

    assert_receive {[:xandra, :cluster, :control_connection, :disconnected], ^telemetry_ref, _, _}
    assert_receive {:DOWN, ^monitor_ref, _, _, {:shutdown, :econnreset}}
  end

  test "deals with StatusChange for known nodes",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    ctrl_conn = start_control_connection!(start_options)

    assert_receive {^mirror_ref, {:discovered_hosts, _peers}}

    send_change_event(ctrl_conn, %StatusChange{effect: "UP", address: {127, 0, 0, 1}, port: @port})

    assert_receive {^mirror_ref, {:host_up, {127, 0, 0, 1}, @port}}, 100

    send_change_event(ctrl_conn, %StatusChange{
      effect: "DOWN",
      address: {127, 0, 0, 1},
      port: @port
    })

    assert_receive {^mirror_ref, {:host_down, {127, 0, 0, 1}, @port}}, 100

    send_change_event(ctrl_conn, %StatusChange{
      effect: "DOWN",
      address: {127, 0, 0, 1},
      port: @port
    })

    assert_receive {^mirror_ref, {:host_down, {127, 0, 0, 1}, @port}}, 100
  end

  # This is just a smoke test, because we actually just refresh the topology after a while here.
  test "deals with TopologyChange NEW_NODE and REMOVED_NODE events",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    ctrl_conn = start_control_connection!(start_options)

    assert_receive {^mirror_ref, {:discovered_hosts, _peers}}

    send_change_event(ctrl_conn, %TopologyChange{
      effect: "NEW_NODE",
      address: {127, 0, 0, 101},
      port: @port
    })

    send_change_event(ctrl_conn, %TopologyChange{
      effect: "REMOVED_NODE",
      address: {127, 0, 0, 102},
      port: @port
    })

    # Wait for the messages to be processed.
    :sys.get_state(ctrl_conn)
  end

  test "ignores TopologyChange events of type MOVED_NODE",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    ctrl_conn = start_control_connection!(start_options)

    assert_receive {^mirror_ref, {:discovered_hosts, _peers}}

    log =
      capture_log(fn ->
        send_change_event(ctrl_conn, %TopologyChange{
          effect: "MOVED_NODE",
          address: {127, 0, 0, 2},
          port: @port
        })

        Process.sleep(100)
      end)

    assert log =~ "Ignored TOPOLOGY_CHANGE event"
  end

  test "sends :discovered_hosts message when refreshing the cluster topology",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    ctrl_conn = start_control_connection!(start_options)
    assert_receive {^mirror_ref, {:discovered_hosts, [%Host{address: {127, 0, 0, 1}}]}}

    new_peers = [
      %Host{address: {192, 168, 1, 1}, port: @port, data_center: "datacenter1"},
      %Host{address: {192, 168, 1, 2}, port: @port, data_center: "datacenter2"}
    ]

    GenServer.cast(ctrl_conn, {:refresh_topology, new_peers})

    assert_receive {^mirror_ref,
                    {:discovered_hosts,
                     [%Host{address: {192, 168, 1, 1}}, %Host{address: {192, 168, 1, 2}}]}}
  end

  test "triggers a topology refresh with the :refresh_topology message",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    ctrl_conn = start_control_connection!(start_options)

    assert_receive {^mirror_ref, {:discovered_hosts, _peers}}

    send(ctrl_conn, :refresh_topology)

    assert_receive {^mirror_ref, {:discovered_hosts, _peers}}
  end

  defp start_control_connection!(start_options, overrides \\ []) do
    options = Keyword.merge(start_options, overrides)
    TestHelper.start_link_supervised!({ControlConnection, options})
  end

  defp mirror(parent, ref) do
    receive do
      message -> send(parent, {ref, message})
    end

    mirror(parent, ref)
  end

  defp send_change_event(ctrl_conn, change_event) do
    assert %{protocol_module: protocol_module, transport: %Transport{socket: socket}} =
             :sys.get_state(ctrl_conn)

    data =
      Frame.new(:event, _options = [])
      |> protocol_module.encode_request(change_event)
      |> Frame.encode(protocol_module)
      |> IO.iodata_to_binary()

    send(ctrl_conn, {:tcp, socket, data})
  end
end
