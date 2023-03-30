defmodule Xandra.Cluster.LoadBalancingPolicy.RandomTest do
  use ExUnit.Case, async: true

  alias Xandra.Cluster.Host
  alias Xandra.Cluster.LoadBalancingPolicy.Random

  setup do
    {:ok, lbp: Random.init([])}
  end

  test "host_added/2 and host_removed/2", %{lbp: lbp} do
    lbp = Random.host_added(lbp, %Host{address: {127, 0, 0, 1}, port: 9042})
    lbp = Random.host_added(lbp, %Host{address: {127, 0, 0, 2}, port: 9042})

    {hosts, _lbp} = Random.hosts_plan(lbp)

    assert Enum.sort(hosts) == [
             %Host{address: {127, 0, 0, 1}, port: 9042},
             %Host{address: {127, 0, 0, 2}, port: 9042}
           ]

    lbp = Random.host_removed(lbp, %Host{address: {127, 0, 0, 2}, port: 9042})

    {hosts, _lbp} = Random.hosts_plan(lbp)
    assert hosts == [%Host{address: {127, 0, 0, 1}, port: 9042}]
  end

  test "host_up/2 and host_down/2", %{lbp: lbp} do
    lbp = Random.host_added(lbp, %Host{address: {127, 0, 0, 1}, port: 9042})
    lbp = Random.host_added(lbp, %Host{address: {127, 0, 0, 2}, port: 9042})

    lbp = Random.host_down(lbp, %Host{address: {127, 0, 0, 2}, port: 9042})

    {hosts, lbp} = Random.hosts_plan(lbp)
    assert hosts == [%Host{address: {127, 0, 0, 1}, port: 9042}]

    lbp = Random.host_up(lbp, %Host{address: {127, 0, 0, 2}, port: 9042})
    {hosts, _lbp} = Random.hosts_plan(lbp)

    assert Enum.sort(hosts) == [
             %Host{address: {127, 0, 0, 1}, port: 9042},
             %Host{address: {127, 0, 0, 2}, port: 9042}
           ]
  end

  test "hosts_plan/1 returns all the hosts that are up in random order", %{lbp: lbp} do
    lbp = Random.host_added(lbp, %Host{address: {127, 0, 0, 1}, port: 9042})
    lbp = Random.host_added(lbp, %Host{address: {127, 0, 0, 2}, port: 9042})
    lbp = Random.host_added(lbp, %Host{address: {127, 0, 0, 3}, port: 9042})

    # Exclude one host.
    lbp = Random.host_down(lbp, %Host{address: {127, 0, 0, 2}, port: 9042})

    {plans, _lbp} = Enum.map_reduce(1..20, lbp, fn _, lbp -> Random.hosts_plan(lbp) end)

    Enum.all?(plans, fn hosts ->
      Enum.sort(hosts) == [
        %Host{address: {127, 0, 0, 1}, port: 9042},
        %Host{address: {127, 0, 0, 3}, port: 9042}
      ]
    end)

    assert Enum.uniq(plans) != plans
  end
end
