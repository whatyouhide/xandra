defmodule Xandra.Cluster.PoolTest do
  use ExUnit.Case, async: true

  alias Xandra.Cluster.Host
  alias Xandra.Cluster.Pool

  describe "prioritize_token_owner/2" do
    test "moves the owner to the front when all hosts are in the same DC" do
      [host1, host2, owner] = plan = [host(1, "dc1"), host(2, "dc1"), host(3, "dc1")]

      assert Pool.prioritize_token_owner(plan, peername(owner)) == [owner, host1, host2]
    end

    test "treats hosts without a DC as being in the same DC" do
      [host1, owner] = plan = [host(1, nil), host(2, nil)]

      assert Pool.prioritize_token_owner(plan, peername(owner)) == [owner, host1]
    end

    test "doesn't promote a remote-DC owner past local-DC hosts" do
      # DCAwareRoundRobin-style plan: local DC first, then the remote DC.
      [local1, local2, owner, remote2] =
        plan = [host(1, "local"), host(2, "local"), host(3, "remote"), host(4, "remote")]

      # The owner is already at the front of its own DC's hosts, so nothing
      # changes.
      assert Pool.prioritize_token_owner(plan, peername(owner)) ==
               [local1, local2, owner, remote2]

      # Same, when the owner has to move to the front of its DC's hosts.
      [local1, local2, remote1, owner] =
        plan = [host(1, "local"), host(2, "local"), host(3, "remote"), host(4, "remote")]

      assert Pool.prioritize_token_owner(plan, peername(owner)) ==
               [local1, local2, owner, remote1]
    end

    test "promotes a local-DC owner to the front of a DC-aware plan" do
      [local1, owner, remote1] = plan = [host(1, "local"), host(2, "local"), host(3, "remote")]

      assert Pool.prioritize_token_owner(plan, peername(owner)) == [owner, local1, remote1]
    end

    test "never promotes the owner past a host in a different DC" do
      # With DC-interleaved plans (like from the Random policy in a multi-DC
      # cluster), the owner only passes the hosts of its own DC that
      # immediately precede it.
      [host1, host2, host3, owner] =
        plan = [host(1, "dc1"), host(2, "dc2"), host(3, "dc1"), host(4, "dc1")]

      assert Pool.prioritize_token_owner(plan, peername(owner)) == [host1, host2, owner, host3]
    end

    test "leaves the plan alone when the owner is not in it" do
      plan = [host(1, "dc1"), host(2, "dc1")]

      assert Pool.prioritize_token_owner(plan, peername(host(9, "dc1"))) == plan
    end
  end

  defp host(last_ip_byte, data_center) do
    %Host{address: {127, 0, 0, last_ip_byte}, port: 9042, data_center: data_center}
  end

  defp peername(%Host{} = host), do: Host.to_peername(host)
end
