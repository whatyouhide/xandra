defmodule Xandra.ClusterTest do
  use ExUnit.Case, async: true

  alias Xandra.TestHelper
  alias Xandra.Cluster.Host

  @protocol_version XandraTest.IntegrationCase.protocol_version()
  @port String.to_integer(System.get_env("CASSANDRA_PORT", "9052"))

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

    test "validates the :name option" do
      message = ~r/invalid value for :name option/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], name: "something something")
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

  describe "connected_hosts/1" do
    test "returns a list of connected %Host{} structs" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert [%Host{} = host] = Xandra.Cluster.connected_hosts(cluster)

      assert host.address == {127, 0, 0, 1}
      assert host.port == @port
      assert host.data_center == "datacenter1"
      assert host.rack == "rack1"
      assert is_binary(host.release_version)
      assert is_binary(host.host_id)
      assert is_binary(host.schema_version)
      assert is_struct(host.tokens, MapSet)
    end
  end

  describe "execute/3" do
    test "executes a query" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert {:ok, %Xandra.Page{}} = Xandra.Cluster.execute(cluster, "SELECT * FROM system.local")

      assert {:ok, %Xandra.Page{}} =
               Xandra.Cluster.execute(
                 cluster,
                 "SELECT * FROM system.local WHERE key = ?",
                 _params = [{"varchar", "local"}],
                 _options = []
               )
    end

    test "returns the result directly for the bang! version" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert %Xandra.Page{} = Xandra.Cluster.execute!(cluster, "SELECT * FROM system.local")

      assert %Xandra.Page{} =
               Xandra.Cluster.execute!(
                 cluster,
                 "SELECT * FROM system.local WHERE key = ?",
                 _params = [{"varchar", "local"}],
                 _options = []
               )
    end
  end

  describe "prepare/3" do
    test "prepares a query" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert {:ok, %Xandra.Prepared{} = prepared} =
               Xandra.Cluster.prepare(cluster, "SELECT * FROM system.local")

      assert {:ok, _page} = Xandra.Cluster.execute(cluster, prepared)
    end

    test "returns the result directly for the bang! version" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert %Xandra.Prepared{} =
               prepared = Xandra.Cluster.prepare!(cluster, "SELECT * FROM system.local")

      assert {:ok, _page} = Xandra.Cluster.execute(cluster, prepared)
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
