defmodule Xandra.Cluster.LoadBalancingPolicy.DCAwareRoundRobin do
  @moduledoc since: "0.15.0"

  alias Xandra.Cluster.Host

  @behaviour Xandra.Cluster.LoadBalancingPolicy

  @init_opts_schema NimbleOptions.new!(
                      local_data_center: [
                        type: {:or, [:string, {:in, [:from_first_peer]}]},
                        default: :from_first_peer,
                        doc: """
                        The local data center. If `:from_first_peer`, this policy picks the local
                        data center from the first peer that is added to the cluster. This is
                        usually the first peer that the control connection connects to, which
                        (if everything goes well) is the first peer listed in the `:nodes`
                        option passed to `Xandra.Cluster.start_link/1`. If you want to force
                        a specific data center to be used as the local data center, you can
                        pass a string as the value of this option (such as `"datacenter1"`).
                        """
                      ]
                    )

  @moduledoc """
  A `Xandra.Cluster.LoadBalancingPolicy` that prefers hosts in a "local" data center.

  This policy uses a **round-robin strategy** to pick hosts, giving precedence to hosts
  in a "local" data center. The local data center is determined by the
  `:local_data_center` option (see below). "Giving precedence" means that
  `hosts_plan/1` will return a list of hosts where first there are all the local
  hosts that are *up*, and then all the remote hosts that are *up*.

  The round-robin strategy is applied to local and remote hosts separately. For example,
  say the local hosts are `LH1`, `LH2`, `LH3`, and the remote hosts are `RH1`, `RH2`, `RH3`.
  The first time, this policy will return `[LH1, RH1, LH2, RH2, LH3, RH3]`. The second time,
  it will return `[LH2, LH3, LH1, RH2, RH3, RH1]`. And so on.

  This policy is available since Xandra *v0.15.0*.

  ## Options

  This policy supports the following initialization options:

  #{NimbleOptions.docs(@init_opts_schema)}
  """

  defstruct [:local_dc, local_hosts: [], remote_hosts: []]

  @impl true
  def init(options) do
    options = NimbleOptions.validate!(options, @init_opts_schema)

    case Keyword.fetch!(options, :local_data_center) do
      :from_first_peer -> %__MODULE__{}
      local_data_center -> %__MODULE__{local_dc: local_data_center}
    end
  end

  @impl true
  def host_added(hosts, new_host)

  def host_added(%__MODULE__{local_dc: nil, local_hosts: []} = state, %Host{} = host) do
    %__MODULE__{state | local_dc: host.data_center, local_hosts: [{host, :up}]}
  end

  def host_added(%__MODULE__{local_dc: local_dc} = state, %Host{data_center: local_dc} = host)
      when is_binary(local_dc) do
    update_in(state.local_hosts, &(&1 ++ [{host, :up}]))
  end

  def host_added(%__MODULE__{} = state, %Host{} = host) do
    update_in(state.remote_hosts, &(&1 ++ [{host, :up}]))
  end

  @impl true
  def host_removed(%__MODULE__{} = state, %Host{} = host) do
    key = if state.local_dc == host.data_center, do: :local_hosts, else: :remote_hosts

    update_in(state, [Access.key!(key)], fn hosts ->
      Enum.reject(hosts, fn {existing_host, _status} -> host_match?(existing_host, host) end)
    end)
  end

  @impl true
  def host_up(%__MODULE__{} = state, %Host{} = host) do
    key = if state.local_dc == host.data_center, do: :local_hosts, else: :remote_hosts

    update_in(state, [Access.key!(key)], fn hosts ->
      Enum.map(hosts, fn {existing_host, status} ->
        if host_match?(existing_host, host), do: {host, :up}, else: {existing_host, status}
      end)
    end)
  end

  @impl true
  def host_down(%__MODULE__{} = state, %Host{} = host) do
    key = if state.local_dc == host.data_center, do: :local_hosts, else: :remote_hosts

    update_in(state, [Access.key!(key)], fn hosts ->
      Enum.map(hosts, fn {existing_host, status} ->
        if host_match?(existing_host, host), do: {host, :down}, else: {existing_host, status}
      end)
    end)
  end

  @impl true
  def hosts_plan(%__MODULE__{} = state) do
    {local_hosts, state} = get_and_update_in(state.local_hosts, &slide/1)
    {remote_hosts, state} = get_and_update_in(state.remote_hosts, &slide/1)

    hosts = for {host, :up} <- local_hosts ++ remote_hosts, do: host

    {hosts, state}
  end

  defp host_match?(%Host{} = host1, %Host{} = host2) do
    host1.address == host2.address and host1.port == host2.port
  end

  defp slide([]), do: {[], []}
  defp slide([head | rest] = list), do: {list, rest ++ [head]}

  # Made public for testing.
  @doc false
  def local_dc(%__MODULE__{local_dc: local_dc}), do: local_dc

  # Made public for testing.
  @doc false
  def hosts(%__MODULE__{local_hosts: hosts}, :local), do: hosts
  def hosts(%__MODULE__{remote_hosts: hosts}, :remote), do: hosts
end
