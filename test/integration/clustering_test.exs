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

  test "basic interactions", %{keyspace: keyspace} do
    call_options = [pool: Xandra.Cluster]
    statement = "USE #{keyspace}"

    log =
      capture_log(fn ->
        start_options = [
          nodes: ["127.0.0.1", "127.0.0.1", "127.0.0.2"],
          name: TestCluster,
          load_balancing: :random,
          underlying_pool: DBConnection.ConnectionPool
        ]

        {:ok, cluster} = Xandra.start_link(call_options ++ start_options)

        assert await_connected(cluster, call_options, &Xandra.execute!(&1, statement))
      end)

    assert log =~ "received request to start another connection pool to the same address"

    assert Xandra.execute!(TestCluster, statement, [], call_options)
  end

  test "priority load balancing", %{keyspace: keyspace} do
    call_options = [pool: Xandra.Cluster]
    start_options = [load_balancing: :priority]
    {:ok, cluster} = Xandra.start_link(call_options ++ start_options)

    assert await_connected(cluster, call_options, &Xandra.execute!(&1, "USE #{keyspace}"))
  end
end
