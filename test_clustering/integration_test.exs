Code.require_file("docker_helpers.exs", __DIR__)

ExUnit.start(trace: true, timeout: 300_000)

ExUnit.after_suite(fn _results ->
  Xandra.TestClustering.DockerHelpers.docker_compose!(["down", "--volumes"])
end)

defmodule Xandra.TestClustering.IntegrationTest do
  use ExUnit.Case

  import Xandra.TestClustering.DockerHelpers

  @protocol_version XandraTest.IntegrationCase.protocol_version()

  setup do
    IO.puts("🚧 Starting Cassandra cluster with docker-compose up -d...")
    nodes = ["seed", "node1", "node2", "node3"]

    start_time = System.system_time()

    docker_compose!(["up", "-d", "--build"] ++ nodes)
    Enum.each(nodes, &wait_for_container_up/1)

    on_exit(fn ->
      IO.write("🛑 Stopping Cassandra cluster...")
      {elapsed_microsec, _} = :timer.tc(fn -> docker_compose!(["stop"] ++ nodes) end)
      IO.write(" Done in #{Float.round(elapsed_microsec / 1_000_000, 3)}s")
    end)

    elapsed_ms =
      System.convert_time_unit(System.system_time() - start_time, :native, :millisecond)

    IO.puts("✅ Done in #{elapsed_ms / 1000}s")

    :ok
  end

  test "if a node goes down, the cluster removes its control connection and pool" do
    conn_count_in_cluster = 4

    {:ok, cluster} =
      Xandra.Cluster.start_link(
        autodiscovery: true,
        nodes: ["node1", "seed"],
        protocol_version: @protocol_version
      )

    wait_for_passing(60_000, fn ->
      assert %Xandra.Cluster{} = cluster_state = :sys.get_state(cluster)
      assert length(cluster_state.node_refs) == conn_count_in_cluster
    end)

    # Wait for all pools to be started.
    wait_for_passing(60_000, fn ->
      assert map_size(:sys.get_state(cluster).pools) == conn_count_in_cluster
    end)

    docker_compose!(["stop", "node2"])

    # Wait for the pool for the stopped node to be stopped.
    wait_for_passing(60_000, fn ->
      assert map_size(:sys.get_state(cluster).pools) == conn_count_in_cluster - 1
    end)
  end

  test "if a node goes down and then rejoins, the cluster readds its control connection and pool" do
    conn_count_in_cluster = 4

    {:ok, cluster} =
      Xandra.Cluster.start_link(
        autodiscovery: true,
        nodes: ["node1", "seed"],
        protocol_version: @protocol_version
      )

    wait_for_passing(60_000, fn ->
      assert %Xandra.Cluster{} = cluster_state = :sys.get_state(cluster)
      assert length(cluster_state.node_refs) == conn_count_in_cluster
    end)

    # Wait for all pools to be started.
    wait_for_passing(60_000, fn ->
      assert map_size(:sys.get_state(cluster).pools) == conn_count_in_cluster
    end)

    docker_compose!(["stop", "node2"])

    # Wait for the pool for the stopped node to be stopped.
    wait_for_passing(60_000, fn ->
      assert map_size(:sys.get_state(cluster).pools) == conn_count_in_cluster - 1
    end)

    docker_compose!(["up", "-d", "node2"])

    # Wait for the pool to the restarted node to be up.
    wait_for_passing(60_000, fn ->
      assert map_size(:sys.get_state(cluster).pools) == conn_count_in_cluster
    end)
  end

  test "connect and discover peers" do
    conn_count_in_cluster = 4

    {:ok, cluster} =
      Xandra.Cluster.start_link(
        autodiscovery: true,
        nodes: ["node1", "seed"],
        protocol_version: @protocol_version
      )

    cluster_state =
      wait_for_passing(60_000, fn ->
        assert %Xandra.Cluster{} = cluster_state = :sys.get_state(cluster)

        assert length(cluster_state.node_refs) == conn_count_in_cluster,
               "expected #{conn_count_in_cluster} elements in node_refs, " <>
                 "got: #{inspect(cluster_state.node_refs)}"

        cluster_state
      end)

    control_conn_peernames =
      wait_for_passing(30_000, fn ->
        for {peername, ref} <- cluster_state.node_refs do
          assert is_reference(ref)
          assert {ip, port} = peername
          assert is_tuple(ip)
          assert port == 9042

          peername
        end
      end)

    assert map_size(cluster_state.pools) == conn_count_in_cluster

    assert Enum.sort(Map.keys(cluster_state.pools)) == Enum.sort(control_conn_peernames)

    pool_children =
      wait_for_passing(60_000, fn ->
        children = Supervisor.which_children(cluster_state.control_conn_supervisor)
        assert length(children) == conn_count_in_cluster
        children
      end)

    for {child_id, pid, _, _} <- pool_children do
      assert {peername, _} = List.keyfind(cluster_state.node_refs, child_id, 1)

      {:connected, control_conn_state} = :sys.get_state(pid)
      assert peername == control_conn_state.peername
    end

    Xandra.Cluster.execute!(cluster, "SELECT * FROM system_schema.keyspaces")
  end

  defp wait_for_passing(time_left, fun) when time_left < 0 do
    fun.()
  end

  defp wait_for_passing(time_left, fun) do
    fun.()
  catch
    _, _ ->
      Process.sleep(100)
      wait_for_passing(time_left - 100, fun)
  end
end
