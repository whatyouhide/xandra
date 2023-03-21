defmodule Xandra.Cluster.LoadBalancingPolicy do
  @moduledoc """
  A behaviour for defining load-balancing policies.

  A load-balancing policy is a way to tell Xandra which nodes to query when connected
  to a cluster.

  For example, a simple load-balancing policy is `Xandra.Cluster.LoadBalancingPolicy.Random`,
  which picks a random node for each query out of the ones that are up.
  """
  @moduledoc since: "0.15.0"

  alias Xandra.Cluster.Host

  @typedoc """
  The state of the load-balancing policy.

  Can be any term and is passed around to all callbacks.
  """
  @typedoc since: "0.15.0"
  @type state() :: term()

  @doc """
  Called to initialize the load-balancing policy.

  Hosts is the initial list of hosts. You can assume that all of them are *up*.
  """
  @doc since: "0.15.0"
  @callback init(hosts :: [Host.t()]) :: state()

  @doc """
  Called when the Cassandra cluster marks `host` as "up".
  """
  @doc since: "0.15.0"
  @callback host_up(state(), host :: Host.t()) :: state()

  @doc """
  Called when the Cassandra cluster marks `host` as "down".
  """
  @doc since: "0.15.0"
  @callback host_down(state(), host :: Host.t()) :: state()

  @doc """
  Called when the Cassandra cluster reports a new host that joined.
  """
  @doc since: "0.15.0"
  @callback host_added(state(), host :: Host.t()) :: state()

  @doc """
  Called when the Cassandra cluster reports a host that left the cluster.
  """
  @doc since: "0.15.0"
  @callback host_removed(state(), host :: Host.t()) :: state()

  @doc """
  Called to return a "plan", which is an enumerable of hosts to query in order.
  """
  @doc since: "0.15.0"
  # TODO: remove the check once we depend on Elixir 1.14+. Enumerable.t/1 was
  # introduced in 1.14.
  if Version.match?(System.version(), ">= 1.14.0") do
    @callback hosts_plan(state()) :: {Enumerable.t(Host.t()), state()}
  else
    @callback hosts_plan(state()) :: {Enumerable.t(), state()}
  end

  ## Private helpers

  @doc false
  def hosts_plan({mod, state}) do
    {hosts, state} = mod.hosts_plan(state)
    {hosts, {mod, state}}
  end

  @doc false
  def update_host({mod, state}, host, event) do
    case event do
      :up -> {mod, mod.host_up(state, host)}
      :down -> {mod, mod.host_down(state, host)}
      :added -> {mod, mod.host_added(state, host)}
      :removed -> {mod, mod.host_removed(state, host)}
    end
  end
end
