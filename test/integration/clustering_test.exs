defmodule ClusteringTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  @call_options [pool: Xandra.Cluster]

  test "basic interactions" do
    nodes = ["127.0.0.1", "127.0.0.1", "127.0.0.2"]
    {:ok, cluster} = Xandra.start_link(@call_options ++ [nodes: nodes])
    log = capture_log(fn ->
      assert await_available(cluster) == Xandra.execute!(cluster, "USE system", [], @call_options)
    end)
    assert log =~ "received request to start another connection pool to the same address"
  end

  defp await_available(cluster, tries \\ 4) do
    try do
      Xandra.execute!(cluster, "USE system", [], @call_options)
    rescue
      Xandra.Cluster.Error ->
        if tries > 0 do
          Process.sleep(50)
          await_available(cluster, tries - 1)
        else
          raise "exceeded maximum number of attempts"
        end
    end
  end
end
