ExUnit.start()

defmodule Xandra.CCTest do
  use ExUnit.Case, async: true

  @tag :connect_and_discover_peers
  test "connect and discover peers" do
    conn_count_in_cluster = 4

    {:ok, cluster} = Xandra.Cluster.start_link(autodiscovery: true, nodes: ["node1", "seed"])

    Process.sleep(2_000)

    assert %Xandra.Cluster{} = cluster_state = :sys.get_state(cluster)

    assert length(cluster_state.control_conn_peername_to_node_ref) == conn_count_in_cluster

    control_conn_peernames =
      for {peername, ref} <- cluster_state.control_conn_peername_to_node_ref do
        assert is_reference(ref)
        assert {ip, port} = peername
        assert is_tuple(ip)
        assert port == 9042

        peername
      end

    assert map_size(cluster_state.pools) == conn_count_in_cluster

    assert Enum.sort(Map.keys(cluster_state.pools)) == Enum.sort(control_conn_peernames)

    for {child_id, pid, _, _} <- Supervisor.which_children(cluster_state.control_conn_supervisor) do
      assert {peername, _} =
               List.keyfind(cluster_state.control_conn_peername_to_node_ref, child_id, 1)

      {:connected, control_conn_state} = :sys.get_state(pid)
      assert peername == control_conn_state.peername
    end

    Xandra.Cluster.execute!(cluster, "SELECT * FROM system_schema.keyspaces")
  end
end
