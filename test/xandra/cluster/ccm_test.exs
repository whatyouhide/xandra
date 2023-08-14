defmodule Xandra.Cluster.CCMTest do
  use ExUnit.Case

  import Xandra.TestHelper, only: [cmd!: 2, wait_for_passing: 2]

  alias Xandra.Cluster.Host

  @moduletag :integration
  @moduletag :ccm

  @cluster_name "xandra_test_cluster"
  @cassandra_version "4.1.3"
  @node_count 3

  setup_all do
    on_exit(fn ->
      ccm("stop")
    end)
  end

  test "integration" do
    validate_ifaddresses_on_macos()

    if ccm("list") =~ "#{@cluster_name}" do
      ccm("switch #{@cluster_name}")
    else
      ccm("create #{@cluster_name} -v #{@cassandra_version}")
      ccm("populate -n #{@node_count}")
    end

    ccm("start")
    ccm("status")

    cluster =
      start_supervised!(
        {Xandra.Cluster, nodes: ["127.0.0.1"], target_pools: 2, sync_connect: 5000}
      )

    assert {:ok, _} = Xandra.Cluster.execute(cluster, "SELECT * FROM system.local", [])

    connected_hosts =
      wait_for_passing(5000, fn ->
        connected_hosts = Xandra.Cluster.connected_hosts(cluster)
        assert length(connected_hosts) == 2
        connected_hosts
      end)

    connected_hosts_set = MapSet.new(connected_hosts, fn %Host{address: address} -> address end)

    assert MapSet.subset?(
             connected_hosts_set,
             MapSet.new([{127, 0, 0, 1}, {127, 0, 0, 2}, {127, 0, 0, 3}])
           )
  end

  defp ccm(args) do
    cmd!("ccm", String.split(args))
  end

  defp validate_ifaddresses_on_macos do
    if :os.type() == {:unix, :darwin} do
      {:ok, addresses} = :inet.getifaddrs()
      assert {~c"lo0", info} = List.keyfind!(addresses, ~c"lo0", 0)

      localhosts = for {:addr, {127, 0, 0, _} = addr} <- info, do: addr

      assert Enum.sort(localhosts) == [
               {127, 0, 0, 1},
               {127, 0, 0, 2},
               {127, 0, 0, 3}
             ]
    end
  end
end
