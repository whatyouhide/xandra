defmodule Xandra.Cluster.ControlConnectionTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Xandra.TestHelper
  alias Xandra.Cluster.{ControlConnection, Host, StatusChange, TopologyChange}

  @protocol_version XandraTest.IntegrationCase.protocol_version()

  setup do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)
    %{mirror_ref: mirror_ref, mirror: mirror}
  end

  test "reporting data upon successful connection", %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      load_balancing_module: Xandra.Cluster.LoadBalancingPolicy.RoundRobin
    ]

    TestHelper.start_link_supervised!({ControlConnection, opts})
    assert_receive {^mirror_ref, {:discovered_peers, [local_peer]}}
    assert %Host{address: {127, 0, 0, 1}, data_center: "datacenter1", rack: "rack1"} = local_peer
  end

  test "trying all the nodes in the contact points", %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["bad-domain", "127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      load_balancing_module: Xandra.Cluster.LoadBalancingPolicy.RoundRobin
    ]

    log =
      capture_log(fn ->
        TestHelper.start_link_supervised!({ControlConnection, opts})
        assert_receive {^mirror_ref, {:discovered_peers, [local_peer]}}
        assert %Host{address: {127, 0, 0, 1}} = local_peer
      end)

    assert log =~ "Error connecting to bad-domain:9042: non-existing domain"
  end

  test "when all contact points are unavailable", %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["bad-domain", "other-bad-domain"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      load_balancing_module: Xandra.Cluster.LoadBalancingPolicy.RoundRobin
    ]

    log =
      capture_log(fn ->
        ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})
        refute_receive {^mirror_ref, _}, 500
        assert {:disconnected, _data} = :sys.get_state(ctrl_conn)
      end)

    assert log =~ "Error connecting to bad-domain:9042: non-existing domain"
    assert log =~ "Error connecting to other-bad-domain:9042: non-existing domain"
  end

  test "reconnecting after the node closes its socket", %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      load_balancing_module: Xandra.Cluster.LoadBalancingPolicy.RoundRobin
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert {{:connected, connected_node}, _data} = :sys.get_state(ctrl_conn)

    send(ctrl_conn, {:tcp_closed, connected_node.socket})
    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert({{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn))
  end

  test "reconnecting after the node's socket errors out",
       %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      load_balancing_module: Xandra.Cluster.LoadBalancingPolicy.RoundRobin
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert {{:connected, connected_node}, _data} = :sys.get_state(ctrl_conn)

    send(ctrl_conn, {:tcp_error, connected_node.socket, :econnreset})
    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert({{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn))
  end

  test "deals with StatusChange for known nodes", %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      load_balancing_module: Xandra.Cluster.LoadBalancingPolicy.RoundRobin
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)

    # No-op: sending a UP event for a node that is already up.
    send(
      ctrl_conn,
      {:__test_event__, %StatusChange{effect: "UP", address: {127, 0, 0, 1}, port: 9042}}
    )

    refute_receive {:host_up, _host}, 100

    # With StatusChange DOWN it notifies the cluster of the host being down.
    send(
      ctrl_conn,
      {:__test_event__, %StatusChange{effect: "DOWN", address: {127, 0, 0, 1}, port: 9042}}
    )

    assert_receive {^mirror_ref, {:host_down, %Host{} = host}}
    assert host.address == {127, 0, 0, 1}
    assert host.port == 9042
    assert host.data_center == "datacenter1"

    # Getting the same DOWN event once more doesn't do anything, the host is already down.
    send(
      ctrl_conn,
      {:__test_event__, %StatusChange{effect: "DOWN", address: {127, 0, 0, 1}, port: 9042}}
    )

    refute_receive {:host_down, _host}, 100

    # Getting StatusChange UP for the node brings it back up and notifies the cluster.
    send(
      ctrl_conn,
      {:__test_event__, %StatusChange{effect: "UP", address: {127, 0, 0, 1}, port: 9042}}
    )

    assert_receive {^mirror_ref, {:host_up, %Host{} = host}}
    assert host.address == {127, 0, 0, 1}
    assert host.port == 9042
    assert host.data_center == "datacenter1"
  end

  @tag :skip
  test "deals with TopologyChange NEW_NODE events", %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      load_balancing_module: Xandra.Cluster.LoadBalancingPolicy.RoundRobin
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)

    send(
      ctrl_conn,
      {:__test_event__, %TopologyChange{effect: "NEW_NODE", address: {127, 0, 0, 2}}}
    )

    flunk("TODO: we need to run this in the cluster")
  end

  test "deals with TopologyChange REMOVED_NODE events", %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      load_balancing_module: Xandra.Cluster.LoadBalancingPolicy.RoundRobin
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)

    send(
      ctrl_conn,
      {:__test_event__,
       %TopologyChange{effect: "REMOVED_NODE", address: {127, 0, 0, 1}, port: 9042}}
    )

    assert_receive {^mirror_ref, {:host_removed, %Host{} = host}}
    assert host.address == {127, 0, 0, 1}
    assert host.port == 9042
    assert host.data_center == "datacenter1"
  end

  test "ignores TopologyChange events of type MOVED_NODE",
       %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      load_balancing_module: Xandra.Cluster.LoadBalancingPolicy.RoundRobin
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)

    log =
      capture_log(fn ->
        send(
          ctrl_conn,
          {:__test_event__, %TopologyChange{effect: "MOVED_NODE", address: {127, 0, 0, 2}}}
        )

        refute_receive _, 100
      end)

    assert log =~ "Ignored TOPOLOGY_CHANGE event"
  end

  defp mirror(parent, ref) do
    receive do
      message -> send(parent, {ref, message})
    end

    mirror(parent, ref)
  end
end
