defmodule Xandra.Cluster.ControlConnectionTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Xandra.Frame
  alias Xandra.TestHelper

  alias Xandra.Cluster.{
    ControlConnection,
    Host,
    LoadBalancingPolicy,
    StatusChange,
    TopologyChange
  }

  @protocol_version XandraTest.IntegrationCase.protocol_version()

  setup context do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)

    registry = :"#{context.test} registry"
    TestHelper.start_link_supervised!({Registry, keys: :unique, name: registry})

    # Base start options for the control connection.
    start_options = [
      cluster: mirror,
      refresh_topology_interval: 60_000,
      autodiscovered_nodes_port: 9042,
      connection_options: [protocol_version: @protocol_version],
      registry: registry,
      load_balancing: {LoadBalancingPolicy.DCAwareRoundRobin, []},
      contact_points: ["127.0.0.1"]
    ]

    %{mirror_ref: mirror_ref, mirror: mirror, registry: registry, start_options: start_options}
  end

  test "reporting data upon successful connection",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    start_control_connection!(start_options)
    assert_receive {^mirror_ref, {:discovered_hosts, [local_peer]}}
    assert %Host{address: {127, 0, 0, 1}, data_center: "datacenter1", rack: "rack1"} = local_peer
  end

  test "trying all the nodes in the contact points",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    log =
      capture_log(fn ->
        start_control_connection!(start_options, contact_points: ["bad-domain", "127.0.0.1"])
        assert_receive {^mirror_ref, {:discovered_hosts, [local_peer]}}
        assert %Host{address: {127, 0, 0, 1}} = local_peer
      end)

    assert log =~ "Control connection failed to connect"
    assert log =~ "xandra_address=bad-domain"
    assert log =~ "xandra_port=9042"
  end

  test "when all contact points are unavailable",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    log =
      capture_log(fn ->
        ctrl_conn =
          start_control_connection!(start_options,
            contact_points: ["bad-domain", "other-bad-domain"]
          )

        refute_receive {^mirror_ref, _}, 500
        assert {:disconnected, _data} = :sys.get_state(ctrl_conn)
      end)

    assert log =~ "Control connection failed to connect"
    assert log =~ "xandra_address=bad-domain"
    assert log =~ "xandra_address=other-bad-domain"
  end

  test "reconnecting after the node closes its socket",
       %{mirror_ref: mirror_ref, mirror: mirror, start_options: start_options} do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:xandra, :cluster, :control_connection, :disconnected],
        [:xandra, :cluster, :control_connection, :connected]
      ])

    assert_telemetry = fn event ->
      assert_receive {[:xandra, :cluster, :control_connection, ^event], ^telemetry_ref,
                      measurements, metadata}

      assert measurements == %{}
      metadata
    end

    ctrl_conn = start_control_connection!(start_options)

    assert_receive {^mirror_ref, {:discovered_hosts, _peers}}

    assert %{cluster_name: nil, cluster_pid: ^mirror, host: %Host{address: {127, 0, 0, 1}}} =
             assert_telemetry.(:connected)

    # Manually simulate closing the socket
    assert {{:connected, connected_node}, _data} = :sys.get_state(ctrl_conn)
    send(ctrl_conn, {:tcp_closed, connected_node.socket})

    assert %{
             cluster_name: nil,
             cluster_pid: ^mirror,
             host: %Host{address: {127, 0, 0, 1}},
             reason: :closed
           } = assert_telemetry.(:disconnected)

    # Assert that it eventually reconnects
    assert_telemetry.(:connected)
  end

  test "reconnecting after the node's socket errors out", %{start_options: start_options} do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:xandra, :cluster, :control_connection, :connected],
        [:xandra, :cluster, :control_connection, :disconnected]
      ])

    ctrl_conn = start_control_connection!(start_options)

    assert_receive {[:xandra, :cluster, :control_connection, :connected], ^telemetry_ref, _, _}
    assert {{:connected, connected_node}, _data} = :sys.get_state(ctrl_conn)

    send(ctrl_conn, {:tcp_error, connected_node.socket, :econnreset})

    assert_receive {[:xandra, :cluster, :control_connection, :disconnected], ^telemetry_ref, _, _}
    assert_receive {[:xandra, :cluster, :control_connection, :connected], ^telemetry_ref, _, _}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)
  end

  test "deals with StatusChange for known nodes",
       %{mirror_ref: mirror_ref, mirror: mirror, start_options: start_options} do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [[:xandra, :cluster, :change_event]])

    ctrl_conn = start_control_connection!(start_options)

    assert_receive {^mirror_ref, {:discovered_hosts, _peers}}
    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, _, _}

    # No-op: sending a UP event for a node that is already up.
    send_change_event(ctrl_conn, %StatusChange{effect: "UP", address: {127, 0, 0, 1}, port: 9042})
    refute_receive {:host_up, _host}, 100

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, measurements, meta}
    assert measurements == %{}
    assert meta.cluster_pid == mirror
    assert %{source: :cassandra, event_type: :host_up, changed: false} = meta
    assert %Host{address: {127, 0, 0, 1}} = meta.host

    # With StatusChange DOWN it notifies the cluster of the host being down.
    send_change_event(ctrl_conn, %StatusChange{
      effect: "DOWN",
      address: {127, 0, 0, 1},
      port: 9042
    })

    assert_receive {^mirror_ref, {:host_down, %Host{} = host}}
    assert host.address == {127, 0, 0, 1}
    assert host.port == 9042
    assert host.data_center == "datacenter1"

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, measurements, meta}
    assert measurements == %{}
    assert meta.cluster_pid == mirror
    assert %{source: :cassandra, event_type: :host_down, changed: true} = meta
    assert %Host{address: {127, 0, 0, 1}} = meta.host

    # Getting the same DOWN event once more doesn't do anything, the host is already down.
    send_change_event(ctrl_conn, %StatusChange{
      effect: "DOWN",
      address: {127, 0, 0, 1},
      port: 9042
    })

    refute_receive {:host_down, _host}, 100

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, measurements, meta}
    assert measurements == %{}
    assert meta.cluster_pid == mirror
    assert %{source: :cassandra, event_type: :host_down, changed: false} = meta
    assert %Host{address: {127, 0, 0, 1}} = meta.host

    # Getting StatusChange UP for the node brings it back up and notifies the cluster.
    send_change_event(ctrl_conn, %StatusChange{effect: "UP", address: {127, 0, 0, 1}, port: 9042})

    assert_receive {^mirror_ref, {:host_up, %Host{} = host}}
    assert host.address == {127, 0, 0, 1}
    assert host.port == 9042
    assert host.data_center == "datacenter1"

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, measurements, meta}
    assert measurements == %{}
    assert meta.cluster_pid == mirror
    assert %{source: :cassandra, event_type: :host_up, changed: true} = meta
    assert %Host{address: {127, 0, 0, 1}} = meta.host
  end

  @tag :skip
  test "deals with TopologyChange NEW_NODE events",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing: {LoadBalancingPolicy.Random, []}
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:host_added, _peer}}

    send_change_event(ctrl_conn, %TopologyChange{
      effect: "NEW_NODE",
      address: {127, 0, 0, 2},
      port: 9042
    })

    flunk("TODO: we need to run this in the cluster")
  end

  test "deals with TopologyChange REMOVED_NODE events",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [[:xandra, :cluster, :change_event]])

    ctrl_conn = start_control_connection!(start_options)

    assert_receive {^mirror_ref, {:discovered_hosts, _peers}}
    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, _, _}

    send_change_event(ctrl_conn, %TopologyChange{
      effect: "REMOVED_NODE",
      address: {127, 0, 0, 1},
      port: 9042
    })

    assert_receive {^mirror_ref, {:host_removed, %Host{} = host}}
    assert host.address == {127, 0, 0, 1}
    assert host.port == 9042
    assert host.data_center == "datacenter1"

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, measurements, meta}
    assert measurements == %{}
    assert %{source: :cassandra, event_type: :host_removed, changed: true} = meta
    assert %Host{address: {127, 0, 0, 1}} = meta.host
  end

  test "ignores TopologyChange events of type MOVED_NODE",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    ctrl_conn = start_control_connection!(start_options)

    assert_receive {^mirror_ref, {:discovered_hosts, _peers}}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)

    log =
      capture_log(fn ->
        send_change_event(ctrl_conn, %TopologyChange{
          effect: "MOVED_NODE",
          address: {127, 0, 0, 2},
          port: 9042
        })

        Process.sleep(100)
      end)

    assert log =~ "Ignored TOPOLOGY_CHANGE event"
  end

  test "sends the right events when refreshing the cluster topology",
       %{mirror_ref: mirror_ref, start_options: start_options} do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [[:xandra, :cluster, :change_event]])

    ctrl_conn = start_control_connection!(start_options)
    assert_receive {^mirror_ref, {:discovered_hosts, [%Host{address: {127, 0, 0, 1}}]}}
    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, _, _}

    new_peers = [
      %Host{address: {192, 168, 1, 1}, port: 9042, data_center: "datacenter1"},
      %Host{address: {192, 168, 1, 2}, port: 9042, data_center: "datacenter2"}
    ]

    :gen_statem.cast(ctrl_conn, {:refresh_topology, new_peers})

    assert_receive {^mirror_ref, {:host_removed, %Host{address: {127, 0, 0, 1}}}}

    assert_receive {^mirror_ref,
                    {:discovered_hosts,
                     [%Host{address: {192, 168, 1, 1}}, %Host{address: {192, 168, 1, 2}}]}}

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                    %{
                      event_type: :host_removed,
                      changed: true,
                      source: :xandra,
                      host: %Host{address: {127, 0, 0, 1}}
                    }}

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                    %{
                      event_type: :host_added,
                      changed: true,
                      source: :xandra,
                      host: %Host{address: {192, 168, 1, 1}}
                    }}

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                    %{
                      event_type: :host_added,
                      changed: true,
                      source: :xandra,
                      host: %Host{address: {192, 168, 1, 2}}
                    }}

    new_peers = [
      %Host{address: {192, 168, 1, 2}, port: 9042, data_center: "datacenter2"},
      %Host{address: {192, 168, 1, 3}, port: 9042, data_center: "datacenter3"}
    ]

    :gen_statem.cast(ctrl_conn, {:refresh_topology, new_peers})

    assert_receive {^mirror_ref, {:host_removed, %Host{address: {192, 168, 1, 1}}}}
    assert_receive {^mirror_ref, {:discovered_hosts, [%Host{address: {192, 168, 1, 3}}]}}

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                    %{
                      event_type: :host_removed,
                      changed: true,
                      source: :xandra,
                      host: %Host{address: {192, 168, 1, 1}}
                    }}

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                    %{
                      event_type: :host_added,
                      changed: true,
                      source: :xandra,
                      host: %Host{address: {192, 168, 1, 3}}
                    }}

    # Send the same list of peers and verify that we don't get any events.
    new_peers = [
      %Host{address: {192, 168, 1, 2}, port: 9042, data_center: "datacenter2"},
      %Host{address: {192, 168, 1, 3}, port: 9042, data_center: "datacenter3"}
    ]

    :gen_statem.cast(ctrl_conn, {:refresh_topology, new_peers})

    refute_receive {^mirror_ref, {_, %Host{address: {192, 168, 1, 1}}}}, 100
    refute_receive {^mirror_ref, {_, %Host{address: {192, 168, 1, 2}}}}, 100
    refute_receive {^mirror_ref, {_, %Host{address: {192, 168, 1, 3}}}}, 100
  end

  test "sends :host_down if all the connections for a node report as disconnected",
       %{mirror_ref: mirror_ref, registry: registry, start_options: start_options} do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [[:xandra, :cluster, :change_event]])

    parent = self()

    [task1_pid, task2_pid] =
      for index <- 1..2 do
        {:ok, task_pid} =
          Task.start_link(fn ->
            key = {{{127, 0, 0, 1}, 9042}, index}
            {:ok, _} = Registry.register(registry, key, :up)
            send(parent, {:ready, self()})

            receive do
              {:disconnect, ctrl_conn} ->
                {_, _} = Registry.update_value(registry, key, fn _ -> :down end)
                send(ctrl_conn, {:disconnected, self()})
            end

            Process.sleep(:infinity)
          end)

        task_pid
      end

    assert_receive {:ready, ^task1_pid}
    assert_receive {:ready, ^task2_pid}

    ctrl_conn = start_control_connection!(start_options)
    assert_receive {^mirror_ref, {:discovered_hosts, [%Host{address: {127, 0, 0, 1}}]}}

    send(task1_pid, {:disconnect, ctrl_conn})
    refute_receive {^mirror_ref, {:host_down, %Host{address: {127, 0, 0, 1}}}}, 100

    send(task2_pid, {:disconnect, ctrl_conn})
    assert_receive {^mirror_ref, {:host_down, %Host{address: {127, 0, 0, 1}}}}

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                    %{
                      event_type: :host_down,
                      changed: true,
                      source: :xandra,
                      host: %Host{address: {127, 0, 0, 1}}
                    }}
  end

  test "performs healthcheck and sends node down message if not registered",
       %{mirror_ref: mirror_ref, registry: registry, start_options: start_options} do
    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [[:xandra, :cluster, :change_event]])

    ctrl_conn = start_control_connection!(start_options)

    assert_receive {^mirror_ref, {:discovered_hosts, [%Host{address: {127, 0, 0, 1}}]}}
    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, _, _}

    parent = self()

    {:ok, task_pid} =
      Task.start_link(fn ->
        key = {{{127, 0, 0, 1}, 9042}, 1}
        {:ok, _} = Registry.register(registry, key, :up)
        send(parent, {:ready, self()})
        Process.sleep(:infinity)
      end)

    assert_receive {:ready, ^task_pid}

    new_peers = [
      %Host{address: {127, 0, 0, 1}, port: 9042, data_center: "datacenter1"},
      %Host{address: {192, 168, 1, 1}, port: 9042, data_center: "datacenter1"}
    ]

    :gen_statem.cast(ctrl_conn, {:refresh_topology, new_peers})

    assert_receive {^mirror_ref, {:discovered_hosts, [%Host{address: {192, 168, 1, 1}}]}}

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                    %{
                      event_type: :host_added,
                      changed: true,
                      source: :xandra,
                      host: %Host{address: {192, 168, 1, 1}}
                    }}

    send(
      ctrl_conn,
      {:healthcheck, %Host{address: {127, 0, 0, 1}, port: 9042, data_center: "datacenter1"}}
    )

    send(
      ctrl_conn,
      {:healthcheck, %Host{address: {192, 168, 1, 1}, port: 9042, data_center: "datacenter1"}}
    )

    refute_receive {^mirror_ref, {:host_down, %Host{address: {127, 0, 0, 1}}}}, 600
    assert_receive {^mirror_ref, {:host_down, %Host{address: {192, 168, 1, 1}}}}

    assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                    %{
                      event_type: :host_down,
                      changed: true,
                      source: :xandra,
                      host: %Host{address: {192, 168, 1, 1}}
                    }}
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
    assert {{:connected, connected_node}, _data} = :sys.get_state(ctrl_conn)

    data =
      Frame.new(:event, _options = [])
      |> connected_node.protocol_module.encode_request(change_event)
      |> Frame.encode(connected_node.protocol_module)
      |> IO.iodata_to_binary()

    send(ctrl_conn, {:tcp, connected_node.socket, data})
  end
end
