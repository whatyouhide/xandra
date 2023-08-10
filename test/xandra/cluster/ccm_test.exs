defmodule Xandra.Cluster.CCMTest do
  use ExUnit.Case

  import Xandra.TestHelper, only: [cmd!: 2, wait_for_passing: 2]

  @moduletag :integration
  @moduletag :ccm

  @cluster_name "xandra_test_cluster"
  @cassandra_version "4.1.3"
  @node_count 3

  test "integration" do
    validate_ifaddresses()

    if ccm("list") =~ "#{@cluster_name}" do
      ccm("switch #{@cluster_name}")
    else
      ccm("create #{@cluster_name} -v #{@cassandra_version}")
      ccm("populate -n #{@node_count}")
    end

    ccm("start")
    ccm("status")

    on_exit(fn ->
      ccm("stop")
    end)

    Process.register(self(), :this_test_process)

    cluster =
      start_supervised!(
        {Xandra.Cluster,
         nodes: ["127.0.0.1"],
         target_pools: 2,
         sync_connect: 5000,
         registry_listeners: [:this_test_process]}
      )

    wait_for_passing(5000, fn ->
      assert map_size(:sys.get_state(cluster).pools) == 2
    end)

    cluster_state = :sys.get_state(cluster)

    assert map_size(cluster_state.pools) == 2

    pool_addresses =
      MapSet.new(cluster_state.pools, fn {{address, port}, _} ->
        assert port == 9042
        address
      end)

    assert [{127, 0, 0, 1}, {127, 0, 0, 2}, {127, 0, 0, 3}]
           |> MapSet.new()
           |> MapSet.intersection(pool_addresses) == pool_addresses

    assert {{:connected, _connected_node}, ctrl_conn_state} =
             :sys.get_state(cluster_state.control_connection)

    assert %{
             {{127, 0, 0, 1}, 9042} => %{host: _host1, status: :up},
             {{127, 0, 0, 2}, 9042} => %{host: _host2, status: :up},
             {{127, 0, 0, 3}, 9042} => %{host: _host3, status: :up}
           } = ctrl_conn_state.peers

    assert_receive {:register, _registry, {{registry_addr1, 9042}, 1}, _pid1, :up}
    assert_receive {:register, _registry, {{registry_addr2, 9042}, 1}, _pid2, :up}

    assert MapSet.subset?(
             MapSet.new([registry_addr1, registry_addr2]),
             MapSet.new([{127, 0, 0, 1}, {127, 0, 0, 2}, {127, 0, 0, 3}])
           )
  end

  defp ccm(args) do
    cmd!("ccm", String.split(args))
  end

  defp validate_ifaddresses do
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
