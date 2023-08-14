defmodule Xandra.ClusterTest do
  use ExUnit.Case, async: true

  alias Xandra.TestHelper
  alias Xandra.Cluster.Host

  defmodule PoolMock do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(opts) do
      {test_pid, test_ref} = :persistent_term.get(:clustering_test_info)
      send(test_pid, {test_ref, __MODULE__, :init_called, opts})
      {:ok, {test_pid, test_ref}}
    end
  end

  defmodule ControlConnectionMock do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(args) do
      {test_pid, test_ref} = :persistent_term.get(:clustering_test_info)
      send(test_pid, {test_ref, __MODULE__, :init_called, args})
      {:ok, {test_pid, test_ref}}
    end

    @impl true
    def handle_info({:started_pool, %Host{}}, state) do
      {:noreply, state}
    end
  end

  setup do
    test_ref = make_ref()
    :persistent_term.put(:clustering_test_info, {self(), test_ref})
    on_exit(fn -> :persistent_term.erase(:clustering_test_info) end)
    %{test_ref: test_ref}
  end

  describe "start_link/1" do
    test "validates the :nodes option" do
      message = ~r/invalid list in :nodes option: invalid value for list element/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["foo:bar"])
      end

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042x"])
      end
    end

    test "validates the :autodiscovered_nodes_port option" do
      message = ~r/invalid value for :autodiscovered_nodes_port option/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], autodiscovered_nodes_port: 99_999)
      end
    end

    test "validates the :load_balancing option" do
      message = ~r/expected :load_balancing option to match at least one given type/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], load_balancing: :inverse)
      end
    end

    test "validates the :refresh_topology_interval option" do
      message = ~r/invalid value for :refresh_topology_interval option/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], refresh_topology_interval: -1)
      end
    end

    test "validates the :target_pools option" do
      message = ~r/invalid value for :target_pools option: expected positive integer/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], target_pools: -1)
      end
    end

    test "validates the :sync_connect option" do
      message = ~r/invalid value for :sync_connect option/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], sync_connect: :foo)
      end
    end
  end

  describe "child_spec/1" do
    test "is provided" do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1.example.com", "node2.example.com"]
      ]

      assert {:ok, _cluster} = start_supervised({Xandra.Cluster, opts})
    end
  end

  describe "stop/1" do
    test "stops the cluster", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1"]
      ]

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})
      assert_control_connection_started(test_ref)

      assert Xandra.Cluster.stop(cluster) == :ok
      refute Process.alive?(cluster)
    end
  end

  defp assert_control_connection_started(test_ref) do
    assert_receive {^test_ref, ControlConnectionMock, :init_called, _start_args}
  end
end
