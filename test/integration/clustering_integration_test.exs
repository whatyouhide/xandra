defmodule ClusteringTest do
  use XandraTest.IntegrationCase

  import ExUnit.CaptureLog

  alias Xandra.TestHelper

  test "basic interactions", %{keyspace: keyspace, start_options: start_options} do
    logger_level = Logger.level()
    on_exit(fn -> Logger.configure(level: logger_level) end)
    Logger.configure(level: :debug)

    statement = "USE #{keyspace}"

    start_options =
      Keyword.merge(start_options,
        load_balancing: :random,
        name: TestCluster,
        nodes: ["127.0.0.1", "127.0.0.1", "127.0.0.2"]
      )

    log =
      capture_log(fn ->
        cluster = start_supervised!({Xandra.Cluster, start_options})
        true = Process.link(cluster)

        assert TestHelper.await_connected(cluster, _options = [], &Xandra.execute!(&1, statement))

        Process.sleep(250)
      end)

    assert log =~
             "Control connection for 127.0.0.1:9042 was already present, shutting this one down"

    assert Xandra.Cluster.execute!(TestCluster, statement, _params = [])
  end

  test "priority load balancing", %{keyspace: keyspace, start_options: start_options} do
    start_options = Keyword.merge(start_options, load_balancing: :priority)
    {:ok, cluster} = Xandra.Cluster.start_link(start_options)

    assert TestHelper.await_connected(
             cluster,
             _options = [],
             &Xandra.execute!(&1, "USE #{keyspace}")
           )
  end
end
