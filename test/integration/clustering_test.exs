defmodule ClusteringTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  @call_options [pool: Xandra.Cluster]

  test "basic interactions" do
    log = capture_log(fn ->
      start_options = [nodes: ["127.0.0.1", "127.0.0.1", "127.0.0.2"], name: TestCluster]
      {:ok, cluster} = Xandra.start_link(@call_options ++ start_options)

      assert await_connected(cluster) == Xandra.execute!(cluster, "USE system", [], @call_options)
    end)
    assert log =~ "received request to start another connection pool to the same address"

    assert Xandra.execute!(TestCluster, "USE system", [], @call_options)
  end

  defp await_connected(cluster, tries \\ 4) do
    try do
      Xandra.execute!(cluster, "USE system", [], @call_options)
    rescue
      Xandra.ConnectionError ->
        if tries > 0 do
          Process.sleep(50)
          await_connected(cluster, tries - 1)
        else
          raise "exceeded maximum number of attempts"
        end
    end
  end
end
