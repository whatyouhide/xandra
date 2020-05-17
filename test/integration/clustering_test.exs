defmodule ClusteringTest do
  use XandraTest.IntegrationCase

  import ExUnit.CaptureLog

  # The way Cosmos DB works, clustering is not supported in this form. Any
  # connection is handled transparently and might undergo internal clustering.
  @moduletag :cosmosdb_unsupported

  def await_connected(cluster, options, fun, tries \\ 4) do
    try do
      Xandra.Cluster.run(cluster, options, fun)
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
    statement = "USE #{keyspace}"

    log =
      capture_log(fn ->
        {:ok, cluster} =
          Xandra.Cluster.start_link(
            nodes: ["127.0.0.1", "127.0.0.1", "127.0.0.2"],
            name: TestCluster,
            load_balancing: :random
          )

        assert await_connected(cluster, _options = [], &Xandra.execute!(&1, statement))

        Process.sleep(250)
      end)

    assert log =~ "received request to start another connection pool to the same address"

    assert Xandra.Cluster.execute!(TestCluster, statement, _params = [])
  end

  test "priority load balancing", %{keyspace: keyspace, start_options: start_options} do
    start_options = Keyword.merge(start_options, load_balancing: :priority, autodiscovery: false)
    {:ok, cluster} = Xandra.Cluster.start_link(start_options)

    assert await_connected(cluster, _options = [], &Xandra.execute!(&1, "USE #{keyspace}"))
  end
end
