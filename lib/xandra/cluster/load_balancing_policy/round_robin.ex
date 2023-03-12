defmodule Xandra.Cluster.LoadBalancingPolicy.RoundRobin do
  @moduledoc """
  TODO
  """
  @moduledoc since: "0.15.0"

  alias Xandra.Cluster.Host

  @behaviour Xandra.Cluster.LoadBalancingPolicy

  @impl true
  def init(hosts) when is_list(hosts) do
    Enum.map(hosts, fn %Host{} = host -> {host, :up} end)
  end

  @impl true
  def hosts_plan(hosts) do
    plan = for {host, :up} <- hosts, do: host
    {plan, slide_list(hosts)}
  end

  @impl true
  def host_added(hosts, new_host) do
    hosts ++ [{new_host, :up}]
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

  defp slide_list([head | rest]) do
    rest ++ [head]
  end
end
