defmodule Xandra.Cluster.ControlConnectionTest do
  use ExUnit.Case

  alias Xandra.Cluster.ControlConnection

  @protocol_version XandraTest.IntegrationCase.protocol_version()

  test "reporting data upon successful connection" do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)

    node_ref = make_ref()

    opts = [
      cluster: mirror,
      node_ref: node_ref,
      address: 'localhost',
      port: 9042,
      connection_options: [protocol_version: @protocol_version],
      autodiscovery: true
    ]

    assert {:ok, _ctrl_conn} = start_supervised({ControlConnection, opts})

    assert_receive {^mirror_ref, {:"$gen_cast", {:activate, _ref, {{127, 0, 0, 1}, 9042}}}}, 2000
    assert_receive {^mirror_ref, {:"$gen_cast", {:discovered_peers, [], "127.0.0.1:9042"}}}, 2000
  end

  test "reconnecting after a disconnection" do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)

    node_ref = make_ref()

    opts = [
      cluster: mirror,
      node_ref: node_ref,
      address: 'localhost',
      port: 9042,
      connection_options: [protocol_version: @protocol_version],
      autodiscovery: false
    ]

    assert {:ok, ctrl_conn} = start_supervised({ControlConnection, opts})

    assert_receive {^mirror_ref, {:"$gen_cast", {:activate, _ref, {{127, 0, 0, 1}, 9042}}}}, 2000

    assert {:connected, data} = :sys.get_state(ctrl_conn)
    send(ctrl_conn, {:tcp_closed, data.socket})

    assert_receive {^mirror_ref,
                    {:"$gen_cast",
                     {:update, {:control_connection_established, {{127, 0, 0, 1}, 9042}}}}},
                   2000
  end

  defp mirror(parent, ref) do
    receive do
      message -> send(parent, {ref, message})
    end

    mirror(parent, ref)
  end
end
