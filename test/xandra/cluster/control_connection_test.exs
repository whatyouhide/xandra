defmodule Xandra.Cluster.ControlConnectionTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Xandra.TestHelper

  alias Xandra.Cluster.{
    ControlConnection,
    Host,
    LoadBalancingPolicy,
    StatusChange,
    TopologyChange
  }

  # A load-balancing policy that just always returns the hosts in the order they were
  # initially given. Great for deterministic tests!
  # TODO: Replace this with any round-robin policy once we have one.
  defmodule ListLBP do
    @behaviour Xandra.Cluster.LoadBalancingPolicy

    @impl true
    def init(hosts), do: hosts

    @impl true
    def host_added(hosts, host), do: hosts ++ [host]

    @impl true
    def host_removed(hosts, host), do: Enum.reject(hosts, &(&1 == host))

    @impl true
    def host_up(hosts, _host), do: hosts

    @impl true
    def host_down(hosts, _host), do: hosts

    @impl true
    def hosts_plan(hosts), do: {hosts, hosts}
  end

  @protocol_version XandraTest.IntegrationCase.protocol_version()

  setup context do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)

    registry = :"#{context.test} registry"
    TestHelper.start_link_supervised!({Registry, keys: :unique, name: registry})

    %{mirror_ref: mirror_ref, mirror: mirror, registry: registry}
  end

  test "reporting data upon successful connection",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: LoadBalancingPolicy.Random
    ]

    TestHelper.start_link_supervised!({ControlConnection, opts})
    assert_receive {^mirror_ref, {:host_added, local_peer}}
    assert %Host{address: {127, 0, 0, 1}, data_center: "datacenter1", rack: "rack1"} = local_peer
  end

  test "trying all the nodes in the contact points",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["bad-domain", "127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: ListLBP
    ]

    log =
      capture_log(fn ->
        TestHelper.start_link_supervised!({ControlConnection, opts})
        assert_receive {^mirror_ref, {:host_added, local_peer}}
        assert %Host{address: {127, 0, 0, 1}} = local_peer
      end)

    assert log =~ "Error connecting: non-existing domain"
    assert log =~ "peer=bad-domain:9042"
  end

  test "when all contact points are unavailable",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["bad-domain", "other-bad-domain"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: ListLBP
    ]

    log =
      capture_log(fn ->
        ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})
        refute_receive {^mirror_ref, _}, 500
        assert {:disconnected, _data} = :sys.get_state(ctrl_conn)
      end)

    assert log =~ "Error connecting: non-existing domain"
    assert log =~ "peer=bad-domain:9042"
    assert log =~ "peer=other-bad-domain:9042"
  end

  test "reconnecting after the node closes its socket",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: LoadBalancingPolicy.Random
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:host_added, _peer}}
    assert {{:connected, connected_node}, _data} = :sys.get_state(ctrl_conn)

    send(ctrl_conn, {:tcp_closed, connected_node.socket})

    TestHelper.wait_for_passing(500, fn ->
      assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)
    end)
  end

  test "reconnecting after the node's socket errors out",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: LoadBalancingPolicy.Random
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:host_added, _peer}}
    assert {{:connected, connected_node}, _data} = :sys.get_state(ctrl_conn)

    send(ctrl_conn, {:tcp_error, connected_node.socket, :econnreset})

    TestHelper.wait_for_passing(500, fn ->
      assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)
    end)
  end

  test "deals with StatusChange for known nodes",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: LoadBalancingPolicy.Random
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:host_added, _peer}}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)

    # No-op: sending a UP event for a node that is already up.
    :gen_statem.cast(
      ctrl_conn,
      {:change_event, %StatusChange{effect: "UP", address: {127, 0, 0, 1}, port: 9042}}
    )

    refute_receive {:host_up, _host}, 100

    # With StatusChange DOWN it notifies the cluster of the host being down.
    :gen_statem.cast(
      ctrl_conn,
      {:change_event, %StatusChange{effect: "DOWN", address: {127, 0, 0, 1}, port: 9042}}
    )

    assert_receive {^mirror_ref, {:host_down, %Host{} = host}}
    assert host.address == {127, 0, 0, 1}
    assert host.port == 9042
    assert host.data_center == "datacenter1"

    # Getting the same DOWN event once more doesn't do anything, the host is already down.
    :gen_statem.cast(
      ctrl_conn,
      {:change_event, %StatusChange{effect: "DOWN", address: {127, 0, 0, 1}, port: 9042}}
    )

    refute_receive {:host_down, _host}, 100

    # Getting StatusChange UP for the node brings it back up and notifies the cluster.
    :gen_statem.cast(
      ctrl_conn,
      {:change_event, %StatusChange{effect: "UP", address: {127, 0, 0, 1}, port: 9042}}
    )

    assert_receive {^mirror_ref, {:host_up, %Host{} = host}}
    assert host.address == {127, 0, 0, 1}
    assert host.port == 9042
    assert host.data_center == "datacenter1"
  end

  @tag :skip
  test "deals with TopologyChange NEW_NODE events",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: LoadBalancingPolicy.Random
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:host_added, _peer}}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)

    :gen_statem.cast(
      ctrl_conn,
      {:change_event, %TopologyChange{effect: "NEW_NODE", address: {127, 0, 0, 2}}}
    )

    flunk("TODO: we need to run this in the cluster")
  end

  test "deals with TopologyChange REMOVED_NODE events",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: LoadBalancingPolicy.Random
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:host_added, _peer}}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)

    :gen_statem.cast(
      ctrl_conn,
      {:change_event,
       %TopologyChange{effect: "REMOVED_NODE", address: {127, 0, 0, 1}, port: 9042}}
    )

    assert_receive {^mirror_ref, {:host_removed, %Host{} = host}}
    assert host.address == {127, 0, 0, 1}
    assert host.port == 9042
    assert host.data_center == "datacenter1"
  end

  test "ignores TopologyChange events of type MOVED_NODE",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: LoadBalancingPolicy.Random
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:host_added, _peer}}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)

    log =
      capture_log(fn ->
        :gen_statem.cast(
          ctrl_conn,
          {:change_event, %TopologyChange{effect: "MOVED_NODE", address: {127, 0, 0, 2}}}
        )

        Process.sleep(100)
      end)

    assert log =~ "Ignored TOPOLOGY_CHANGE event"
  end

  test "sends the right events when refreshing the cluster topology",
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: LoadBalancingPolicy.Random
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})
    assert_receive {^mirror_ref, {:host_added, %Host{address: {127, 0, 0, 1}}}}

    new_peers = [
      %Host{address: {192, 168, 1, 1}, port: 9042, data_center: "datacenter1"},
      %Host{address: {192, 168, 1, 2}, port: 9042, data_center: "datacenter2"}
    ]

    :gen_statem.cast(ctrl_conn, {:refresh_topology, new_peers})

    assert_receive {^mirror_ref, {:host_removed, %Host{address: {127, 0, 0, 1}}}}
    assert_receive {^mirror_ref, {:host_added, %Host{address: {192, 168, 1, 1}}}}
    assert_receive {^mirror_ref, {:host_added, %Host{address: {192, 168, 1, 2}}}}

    new_peers = [
      %Host{address: {192, 168, 1, 2}, port: 9042, data_center: "datacenter2"},
      %Host{address: {192, 168, 1, 3}, port: 9042, data_center: "datacenter3"}
    ]

    :gen_statem.cast(ctrl_conn, {:refresh_topology, new_peers})

    assert_receive {^mirror_ref, {:host_removed, %Host{address: {192, 168, 1, 1}}}}
    assert_receive {^mirror_ref, {:host_added, %Host{address: {192, 168, 1, 3}}}}

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
       %{mirror_ref: mirror_ref, mirror: mirror, registry: registry} do
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

    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042,
      refresh_topology_interval: 60_000,
      registry: registry,
      load_balancing_module: LoadBalancingPolicy.Random
    ]

    ctrl_conn = TestHelper.start_link_supervised!({ControlConnection, opts})
    assert_receive {^mirror_ref, {:host_added, %Host{address: {127, 0, 0, 1}}}}

    send(task1_pid, {:disconnect, ctrl_conn})
    refute_receive {^mirror_ref, {:host_down, %Host{address: {127, 0, 0, 1}}}}, 100

    send(task2_pid, {:disconnect, ctrl_conn})
    assert_receive {^mirror_ref, {:host_down, %Host{address: {127, 0, 0, 1}}}}
  end

  defp mirror(parent, ref) do
    receive do
      message -> send(parent, {ref, message})
    end

    mirror(parent, ref)
  end
end
