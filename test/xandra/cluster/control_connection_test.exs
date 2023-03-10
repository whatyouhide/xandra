defmodule Xandra.Cluster.ControlConnectionTest do
  use ExUnit.Case

  alias Xandra.Cluster.ControlConnection

  @protocol_version XandraTest.IntegrationCase.protocol_version()

  test "reporting data upon successful connection" do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)

    opts = [
      cluster: mirror,
      contact_points: ["127.0.0.1"],
      connection_options: [protocol_version: @protocol_version],
      autodiscovered_nodes_port: 9042
    ]

    assert {:ok, _ctrl_conn} = start_supervised({ControlConnection, opts})
    assert_receive {^mirror_ref, {:discovered_peers, peers}}
    assert peers == [{{127, 0, 0, 1}, 9042}]
  end

  test "reconnecting after the node closes its socket" do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)

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

  test "reconnecting after the node's socket errors out" do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)

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
