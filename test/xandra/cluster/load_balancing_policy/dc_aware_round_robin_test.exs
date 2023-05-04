defmodule Xandra.Cluster.LoadBalancingPolicy.DCAwareRoundRobinTest do
  use ExUnit.Case, async: true

  alias Xandra.Cluster.Host
  alias Xandra.Cluster.LoadBalancingPolicy.DCAwareRoundRobin

  describe "init/1" do
    test "validates the :local_data_center option" do
      assert_raise NimbleOptions.ValidationError, ~r/expected :local_data_center option/, fn ->
        DCAwareRoundRobin.init(local_data_center: :invalid)
      end
    end
  end

  describe "host_added/2" do
    test "with no local DC set yet" do
      lbp = DCAwareRoundRobin.init(local_data_center: :from_first_peer)

      host1 = host("127.0.0.1:9042", "dc1")
      lbp = DCAwareRoundRobin.host_added(lbp, host1)

      assert DCAwareRoundRobin.local_dc(lbp) == "dc1"
      assert DCAwareRoundRobin.hosts(lbp, :local) == [{host1, :up}]

      # If we add another host with a different DC, the LBP's DC stays the same.
      host2 = host("127.0.0.2:9042", "dc2")
      lbp = DCAwareRoundRobin.host_added(lbp, host2)
      assert DCAwareRoundRobin.local_dc(lbp) == "dc1"
      assert DCAwareRoundRobin.hosts(lbp, :local) == [{host1, :up}]
      assert DCAwareRoundRobin.hosts(lbp, :remote) == [{host2, :up}]
    end

    test "with a forced local DC" do
      lbp = DCAwareRoundRobin.init(local_data_center: "dc1")

      host = host("127.0.0.2:9042", "dc2")
      lbp = DCAwareRoundRobin.host_added(lbp, host)
      assert DCAwareRoundRobin.local_dc(lbp) == "dc1"
      assert DCAwareRoundRobin.hosts(lbp, :local) == []
      assert DCAwareRoundRobin.hosts(lbp, :remote) == [{host, :up}]
    end
  end

  describe "host_removed/2" do
    test "with a local host" do
      lbp = DCAwareRoundRobin.init(local_data_center: "dc1")
      host = host("127.0.0.1:9042", "dc1")
      lbp = DCAwareRoundRobin.host_added(lbp, host)

      lbp = DCAwareRoundRobin.host_removed(lbp, host)

      assert DCAwareRoundRobin.local_dc(lbp) == "dc1"
      assert DCAwareRoundRobin.hosts(lbp, :local) == []
      assert DCAwareRoundRobin.hosts(lbp, :remote) == []
    end

    test "with a remote host" do
      lbp = DCAwareRoundRobin.init(local_data_center: "dc1")
      host = host("127.0.0.1:9042", "dc2")
      lbp = DCAwareRoundRobin.host_added(lbp, host)

      lbp = DCAwareRoundRobin.host_removed(lbp, host)

      assert DCAwareRoundRobin.local_dc(lbp) == "dc1"
      assert DCAwareRoundRobin.hosts(lbp, :local) == []
      assert DCAwareRoundRobin.hosts(lbp, :remote) == []
    end
  end

  describe "host_up/2 and host_down/2" do
    test "with a local host" do
      lbp = DCAwareRoundRobin.init(local_data_center: "dc1")
      host = host("127.0.0.1:9042", "dc1")
      lbp = DCAwareRoundRobin.host_added(lbp, host)

      lbp = DCAwareRoundRobin.host_down(lbp, host)
      assert DCAwareRoundRobin.hosts(lbp, :local) == [{host, :down}]

      lbp = DCAwareRoundRobin.host_up(lbp, host)
      assert DCAwareRoundRobin.hosts(lbp, :local) == [{host, :up}]
    end

    test "with a remote host" do
      lbp = DCAwareRoundRobin.init(local_data_center: "dc1")
      host = host("127.0.0.1:9042", "dc2")
      lbp = DCAwareRoundRobin.host_added(lbp, host)

      lbp = DCAwareRoundRobin.host_down(lbp, host)
      assert DCAwareRoundRobin.hosts(lbp, :remote) == [{host, :down}]

      lbp = DCAwareRoundRobin.host_up(lbp, host)
      assert DCAwareRoundRobin.hosts(lbp, :remote) == [{host, :up}]
    end
  end

  describe "query_plan/1" do
    test "with no hosts" do
      lbp = DCAwareRoundRobin.init(local_data_center: "dc1")
      assert {[], _lbo} = DCAwareRoundRobin.query_plan(lbp)
    end

    test "with mixed local and remote hosts, round-robins through them" do
      lbp = DCAwareRoundRobin.init(local_data_center: "dc1")
      local_host1 = host("127.0.0.1:9042", "dc1")
      local_host2 = host("127.0.0.2:9042", "dc1")
      local_host3 = host("127.0.0.3:9042", "dc1")
      remote_host1 = host("128.0.0.1:9042", "dc2")
      remote_host2 = host("128.0.0.2:9042", "dc2")
      remote_host3 = host("128.0.0.3:9042", "dc2")

      lbp =
        Enum.reduce(
          [local_host1, local_host2, local_host3, remote_host1, remote_host2, remote_host3],
          lbp,
          &DCAwareRoundRobin.host_added(&2, &1)
        )

      assert %DCAwareRoundRobin{
               local_dc: "dc1",
               local_hosts: local_hosts,
               remote_hosts: remote_hosts
             } = lbp

      assert local_hosts ==
               Enum.map([local_host1, local_host2, local_host3], fn host -> {host, :up} end)

      assert remote_hosts ==
               Enum.map([remote_host1, remote_host2, remote_host3], fn host -> {host, :up} end)

      assert {hosts, lbp} = DCAwareRoundRobin.query_plan(lbp)

      # Doesn't return any host when none is connected
      assert hosts == []

      lbp =
        Enum.reduce(
          [local_host1, remote_host1],
          lbp,
          &DCAwareRoundRobin.host_connected(&2, &1)
        )

      assert {hosts, lbp} = DCAwareRoundRobin.query_plan(lbp)

      # Only returns hosts that are connected
      assert hosts == [
               local_host1,
               remote_host1
             ]

      lbp =
        Enum.reduce(
          [local_host2, local_host3, remote_host2, remote_host3],
          lbp,
          &DCAwareRoundRobin.host_connected(&2, &1)
        )

      assert {hosts, lbp} = DCAwareRoundRobin.query_plan(lbp)

      assert hosts == [
               local_host3,
               local_host1,
               local_host2,
               remote_host3,
               remote_host1,
               remote_host2
             ]

      assert {hosts, lbp} = DCAwareRoundRobin.query_plan(lbp)

      assert hosts == [
               local_host1,
               local_host2,
               local_host3,
               remote_host1,
               remote_host2,
               remote_host3
             ]

      # Removes node when node is down.
      lbp = DCAwareRoundRobin.host_down(lbp, local_host2)
      lbp = DCAwareRoundRobin.host_down(lbp, remote_host3)

      assert {hosts, _lbp} = DCAwareRoundRobin.hosts_plan(lbp)
      assert hosts == [local_host3, local_host1, remote_host2, remote_host1]
    end
  end

  describe "hosts_plan/1" do
    test "with no hosts" do
      lbp = DCAwareRoundRobin.init(local_data_center: "dc1")
      assert {[], _lbo} = DCAwareRoundRobin.hosts_plan(lbp)
    end

    test "with mixed local and remote hosts, round-robins through them" do
      lbp = DCAwareRoundRobin.init(local_data_center: "dc1")
      local_host1 = host("127.0.0.1:9042", "dc1")
      local_host2 = host("127.0.0.2:9042", "dc1")
      local_host3 = host("127.0.0.3:9042", "dc1")
      remote_host1 = host("128.0.0.1:9042", "dc2")
      remote_host2 = host("128.0.0.2:9042", "dc2")
      remote_host3 = host("128.0.0.3:9042", "dc2")

      lbp =
        Enum.reduce(
          [local_host1, local_host2, local_host3, remote_host1, remote_host2, remote_host3],
          lbp,
          &DCAwareRoundRobin.host_added(&2, &1)
        )

      lbp =
        Enum.reduce(
          [local_host2, local_host3, remote_host2],
          lbp,
          &DCAwareRoundRobin.host_connected(&2, &1)
        )

      assert {hosts, lbp} = DCAwareRoundRobin.hosts_plan(lbp)

      # Returns all :up and :connected nodes
      assert hosts == [
               local_host1,
               local_host2,
               local_host3,
               remote_host1,
               remote_host2,
               remote_host3
             ]
    end
  end

  defp host(address_and_port, dc) do
    [address, port] = String.split(address_and_port, ":", parts: 2)

    %Host{
      address: address |> String.to_charlist() |> :inet.parse_address(),
      port: String.to_integer(port),
      data_center: dc
    }
  end
end
