defmodule Xandra.TestHelper do
  import ExUnit.Assertions

  @spec await_cluster_connected(pid, pos_integer) :: :ok
  def await_cluster_connected(cluster, tries \\ 10) when is_pid(cluster) do
    fun = &Xandra.execute!(&1, "SELECT * FROM system.local")

    case Xandra.Cluster.run(cluster, _options = [], fun) do
      {:error, %Xandra.ConnectionError{} = error} -> raise error
      _other -> :ok
    end
  rescue
    Xandra.ConnectionError ->
      if tries > 0 do
        Process.sleep(100)
        await_cluster_connected(cluster, tries - 1)
      else
        flunk("exceeded maximum number of attempts")
      end
  end

  # TODO: remove once we have ExUnit.Callbacks.start_link_supervised!/1 (Elixir 1.14+).
  if function_exported?(ExUnit.Callbacks, :start_link_supervised!, 2) do
    defdelegate start_link_supervised!(spec), to: ExUnit.Callbacks
    defdelegate start_link_supervised!(spec, opts), to: ExUnit.Callbacks
  else
    @spec start_link_supervised!(Supervisor.child_spec(), keyword) :: pid
    def start_link_supervised!(spec, opts \\ []) do
      pid = ExUnit.Callbacks.start_supervised!(spec, opts)
      true = Process.link(pid)
      pid
    end
  end

  @spec wait_for_passing(pos_integer, (() -> result)) :: result when result: var
  def wait_for_passing(time_left, fun)

  def wait_for_passing(time_left, fun) when time_left < 0, do: fun.()

  @sleep_interval 50
  def wait_for_passing(time_left, fun) do
    fun.()
  catch
    _, _ ->
      Process.sleep(@sleep_interval)
      wait_for_passing(time_left - @sleep_interval, fun)
  end

  # A load-balancing policy that just always returns the hosts in the order they were
  # initially given. Great for deterministic tests!
  # TODO: Replace this with any round-robin policy once we have one.
  defmodule ListLBP do
    @behaviour Xandra.Cluster.LoadBalancingPolicy

    @impl true
    def init([] = _opts), do: []

    @impl true
    def host_added(hosts, host), do: hosts ++ [host]

    @impl true
    def host_removed(hosts, host), do: Enum.reject(hosts, &(&1 == host))

    @impl true
    def host_up(hosts, _host), do: hosts

    @impl true
    def host_down(hosts, _host), do: hosts

    @impl true
    def hosts_plan(hosts), do: {hosts, hosts}
  end
end
