defmodule Xandra.Cluster.ControlConnectionTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Xandra.Cluster.ControlConnection

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
      autodiscovered_nodes_port: 9042
    ]

    start_link_supervised!({ControlConnection, opts})
    assert_receive {^mirror_ref, {:discovered_peers, peers}}
    assert peers == [{{127, 0, 0, 1}, 9042}]
  end

  test "trying all the nodes in the contact points", %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["bad-domain", "127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042
    ]

    log =
      capture_log(fn ->
        start_link_supervised!({ControlConnection, opts})
        assert_receive {^mirror_ref, {:discovered_peers, peers}}
        assert peers == [{{127, 0, 0, 1}, 9042}]
      end)

    assert log =~ "Error connecting to bad-domain:9042: non-existing domain"
  end

  test "when all contact points are unavailable", %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["bad-domain", "other-bad-domain"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042
    ]

    log =
      capture_log(fn ->
        ctrl_conn = start_link_supervised!({ControlConnection, opts})
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
      autodiscovered_nodes_port: 9042
    ]

    ctrl_conn = start_link_supervised!({ControlConnection, opts})

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
      autodiscovered_nodes_port: 9042
    ]

    ctrl_conn = start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert {{:connected, connected_node}, _data} = :sys.get_state(ctrl_conn)

    send(ctrl_conn, {:tcp_error, connected_node.socket, :econnreset})
    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert({{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn))
  end

  @tag :skip
  test "forwards cluster events to the cluster", %{mirror_ref: mirror_ref, mirror: mirror} do
    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042
    ]

    ctrl_conn = start_link_supervised!({ControlConnection, opts})

    assert_receive {^mirror_ref, {:discovered_peers, [_peer]}}
    assert {{:connected, _connected_node}, _data} = :sys.get_state(ctrl_conn)

    flunk("TODO: simulate an event being sent to the control connection")
  end

  defp mirror(parent, ref) do
    receive do
      message -> send(parent, {ref, message})
    end

    mirror(parent, ref)
  end

  # TODO: remove once we have ExUnit.Callbacks.start_link_supervised!/1 (Elixir 1.14+).
  if not function_exported?(ExUnit.Callbacks, :start_link_supervised!, 1) do
    defp start_link_supervised!(child_spec) do
      pid = start_supervised!(child_spec)
      true = Process.link(pid)
      pid
    end
  end
end
