defmodule Xandra.Cluster.LoadBalancingPolicy.Random do
  @moduledoc """
  A simple `Xandra.Cluster.LoadBalancingPolicy` that picks hosts at random.

  This load-balancing policy doesn't make any attempt to be smart: it doesn't take
  data center or tokens into consideration, and considers all available nodes.

  This policy is available since Xandra *v0.15.0*.
  """
  @moduledoc since: "0.15.0"

  alias Xandra.Cluster.Host

  @behaviour Xandra.Cluster.LoadBalancingPolicy

  @impl true
  def init([] = _options) do
    []
  end

  @impl true
  def host_added(hosts, new_host) do
    Enum.uniq_by([{new_host, :reported_up}] ++ hosts, fn {host, _status} -> Host.to_peername(host) end)
  end

  @impl true
  def host_reported_up(hosts, new_host) do
    Enum.map(hosts, fn {host, status} ->
      if host_match?(host, new_host), do: {host, :reported_up}, else: {host, status}
    end)
  end

  @impl true
  def host_removed(hosts, host) do
    Enum.reject(hosts, fn {existing_host, _status} -> host_match?(existing_host, host) end)
  end

  @impl true
  def host_up(hosts, new_host) do
    Enum.map(hosts, fn {host, status} ->
      if host_match?(host, new_host), do: {host, :up}, else: {host, status}
    end)
  end

  @impl true
  def host_down(hosts, host_down) do
    Enum.map(hosts, fn {host, status} ->
      if host_match?(host, host_down), do: {host, :down}, else: {host, status}
    end)
  end

  @impl true
  def hosts_plan(hosts) do
    up_hosts = for {host, :up} <- hosts, do: host
    {Enum.shuffle(up_hosts), hosts}
  end

  @impl true
  def reported_up_hosts_plan(hosts) do
    reported_up_hosts = for {host, status} when status in [:up, :reported_up] <- hosts, do: host
    {Enum.shuffle(reported_up_hosts), hosts}
  end

  defp host_match?(%Host{} = host1, %Host{} = host2) do
    host1.address == host2.address and host1.port == host2.port
  end
end
