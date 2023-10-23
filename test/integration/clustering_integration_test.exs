defmodule ClusteringTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.TestHelper

  @tag start_conn: false
  test "basic interactions with a single-node cluster",
       %{keyspace: keyspace, start_options: start_options} do
    start_options =
      Keyword.merge(start_options,
        load_balancing: :random,
        name: TestCluster
      )

    cluster = start_link_supervised!({Xandra.Cluster, start_options})
    TestHelper.await_cluster_connected(cluster)

    assert Xandra.Cluster.execute!(TestCluster, "USE #{keyspace}", _params = [])
  end
end
