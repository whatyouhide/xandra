defmodule Xandra.Cluster.LoadBalancingPolicy.Random do
  @moduledoc """
  A simple `Xandra.Cluster.LoadBalancingPolicy` that picks hosts at random.

  This load-balancing policy doesn't make any attempt to be smart: it doesn't take
  data center or tokens into consideration, and considers all available nodes.
  """
  @moduledoc since: "0.15.0"

  alias Xandra.Cluster.Host

  @behaviour Xandra.Cluster.LoadBalancingPolicy

  @impl true
  def init(hosts) do
    Enum.map(hosts, &{&1, :up})
  end

  @impl true
  def host_added(hosts, new_host) do
    Enum.uniq_by([{new_host, :up}] ++ hosts, fn {host, _status} -> {host.address, host.port} end)
  end

  @impl true
  def host_removed(hosts, host) do
    Enum.reject(hosts, fn {%Host{address: address, port: port}, _status} ->
      address == host.address and port == host.port
    end)
  end

  @impl true
  def host_up(hosts, new_host) do
    Enum.map(hosts, fn {host, status} ->
      if host.address == new_host.address and host.port == new_host.port do
        {host, :up}
      else
        {host, status}
      end
    end)
  end

  @impl true
  def host_down(hosts, host_down) do
    Enum.map(hosts, fn {host, status} ->
      if host.address == host_down.address and host.port == host_down.port do
        {host, :down}
      else
        {host, status}
      end
    end)
  end

  @impl true
  def hosts_plan(hosts) do
    up_hosts = for {host, :up} <- hosts, do: host
    {Enum.shuffle(up_hosts), hosts}
  end
end
