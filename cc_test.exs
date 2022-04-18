ExUnit.start()

defmodule Xandra.CCTest do
  use ExUnit.Case, async: true

  test "testing" do
    {:ok, cluster} = Xandra.Cluster.start_link(autodiscovery: true, nodes: ["seed", "node1"])

    Process.sleep(20_000)


    Xandra.Cluster.execute!(cluster, "SELECT * FROM system_schema.keyspaces")
  end
end
