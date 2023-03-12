defmodule Xandra.Cluster.LoadBalancingPolicy do
  @moduledoc """
  TODO
  """
  @moduledoc since: "0.15.0"

  alias Xandra.Cluster.Host

  @type state() :: term()

  @callback init(hosts :: [Host.t()]) :: state()

  @callback host_up(state(), Host.t()) :: state()

  @callback host_down(state(), Host.t()) :: state()

  @callback host_added(state(), Host.t()) :: state()

  @callback host_removed(state(), Host.t()) :: state()

  @callback hosts_plan(state()) :: {Enumerable.t(Host.t()), state()}
end
