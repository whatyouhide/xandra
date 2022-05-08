defmodule ClusteringTest do
  use XandraTest.IntegrationCase

  import ExUnit.CaptureLog

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
    logger_level = Logger.level()
    on_exit(fn -> Logger.configure(level: logger_level) end)
    Logger.configure(level: :debug)

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

    assert log =~
             "Control connection for 127.0.0.1:9042 was already present, shutting this one down"

    assert Xandra.Cluster.execute!(TestCluster, statement, _params = [])
  end

  test "priority load balancing", %{keyspace: keyspace, start_options: start_options} do
    start_options = Keyword.merge(start_options, load_balancing: :priority, autodiscovery: false)
    {:ok, cluster} = Xandra.Cluster.start_link(start_options)

    assert await_connected(cluster, _options = [], &Xandra.execute!(&1, "USE #{keyspace}"))
  end
end
