defmodule ClusteringTest do
  use XandraTest.IntegrationCase

  import ExUnit.CaptureLog

  def await_connected(cluster, options, fun, tries \\ 4) do
    try do
      Xandra.run(cluster, options, fun)
    rescue
      Xandra.ConnectionError ->
        if tries > 0 do
          Process.sleep(50)
          await_connected(cluster, options, fun, tries - 1)
        else
          raise "exceeded maximum number of attempts"
        end
    end
  end

  @tag :cassandra_specific
  test "basic interactions on Cassandra", %{keyspace: keyspace} do
    call_options = [pool: Xandra.Cluster]
    statement = "USE #{keyspace}"

    log =
      capture_log(fn ->
        start_options = [
          nodes: ["127.0.0.1", "127.0.0.1", "127.0.0.2"],
          name: TestCluster,
          load_balancing: :random
        ]

        {:ok, cluster} = Xandra.start_link(call_options ++ start_options)

        assert await_connected(cluster, call_options, &Xandra.execute!(&1, statement))
      end)

    assert log =~ "received request to start another connection pool to the same address"

    assert Xandra.execute!(TestCluster, statement, [], call_options)
  end

  @tag :scylla_spacific
  test "basic interactions on Scylla", %{keyspace: keyspace} do
    call_options = [pool: Xandra.Cluster]
    statement = "USE #{keyspace}"

    log =
      capture_log(fn ->
        start_options = [
          nodes: ["127.0.0.1:9043", "127.0.0.1:9043", "127.0.0.2:9043"],
          name: TestCluster,
          load_balancing: :random
        ]

        {:ok, cluster} = Xandra.start_link(call_options ++ start_options)

        assert await_connected(cluster, call_options, &Xandra.execute!(&1, statement))
      end)

    assert log =~ "received request to start another connection pool to the same address"

    assert Xandra.execute!(TestCluster, statement, [], call_options)
  end

  test "priority load balancing", %{start_options: start_options, keyspace: keyspace} do
    call_options = [pool: Xandra.Cluster]
    start_options = [load_balancing: :priority] ++ start_options
    {:ok, cluster} = Xandra.start_link(call_options ++ start_options)

    assert await_connected(cluster, call_options, &Xandra.execute!(&1, "USE #{keyspace}"))
  end
end
