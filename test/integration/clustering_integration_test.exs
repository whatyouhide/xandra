defmodule ClusteringTest do
  use XandraTest.IntegrationCase

  alias Xandra.TestHelper

  test "basic interactions", %{keyspace: keyspace, start_options: start_options} do
    logger_level = Logger.level()
    on_exit(fn -> Logger.configure(level: logger_level) end)

    statement = "USE #{keyspace}"

    start_options =
      Keyword.merge(start_options,
        load_balancing: :random,
        name: TestCluster
      )

    cluster = start_link_supervised!({Xandra.Cluster, start_options})
    TestHelper.await_cluster_connected(cluster)

    assert Xandra.Cluster.execute!(TestCluster, statement, _params = [])
  end
end
