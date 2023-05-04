defmodule Xandra.ClusterTest do
  use ExUnit.Case

  alias Xandra.TestHelper
  alias Xandra.Cluster.Host
  alias Xandra.Cluster.LoadBalancingPolicy

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
    def handle_info({:healthcheck, %Host{}}, state) do
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
      message =
        ~r/invalid value for :load_balancing option: expected one of \[:priority, :random\]/

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

  describe "in the starting phase" do
    test "starts a single control connection with the right contact points", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1.example.com", "node2.example.com"]
      ]

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert_receive {^test_ref, ControlConnectionMock, :init_called, args}
      assert args.contact_points == [{'node1.example.com', 9042}, {'node2.example.com', 9042}]
      assert args.cluster == cluster
    end

    test "starts one pool per node up to :target_pools", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1.example.com"],
        target_pools: 2,
        load_balancing_policy: {LoadBalancingPolicy.DCAwareRoundRobin, []}
      ]

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})
      assert_control_connection_started(test_ref)

      discovered_peers(cluster, [
        host1 = %Host{address: {199, 0, 0, 1}, port: 9042},
        host2 = %Host{address: {199, 0, 0, 2}, port: 9042},
        %Host{address: {199, 0, 0, 3}, port: 9042}
      ])

      # Assert that the cluster starts a pool for each discovered peer up to :target_pools.
      assert_pool_started(test_ref, host1)
      assert_pool_started(test_ref, host2)
      refute_other_pools_started(test_ref)
    end
  end

  test "handles status change events", %{test_ref: test_ref} do
    host1 = %Host{address: {199, 0, 0, 1}, port: 9042}
    host2 = %Host{address: {199, 0, 0, 2}, port: 9042}
    peername1 = Host.to_peername(host1)
    peername2 = Host.to_peername(host2)

    opts = [
      xandra_module: PoolMock,
      control_connection_module: ControlConnectionMock,
      nodes: ["node1"],
      target_pools: 1,
      load_balancing_policy: {LoadBalancingPolicy.DCAwareRoundRobin, []}
    ]

    cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

    assert_control_connection_started(test_ref)

    discovered_peers(cluster, [host1, host2])

    assert_pool_started(test_ref, host1)
    refute_other_pools_started(test_ref)

    assert %{pool_supervisor: pool_sup, pools: %{^peername1 => pool1}} = :sys.get_state(cluster)

    assert [{^peername1, ^pool1, :worker, _}] = Supervisor.which_children(pool_sup)
    assert Process.alive?(pool1)

    pool_monitor_ref = Process.monitor(pool1)

    # StatusChange DOWN:
    send(cluster, {:host_down, host1})
    assert_receive {:DOWN, ^pool_monitor_ref, _, _, _}

    TestHelper.wait_for_passing(100, fn ->
      assert {_, :undefined, _, _} =
               List.keyfind(Supervisor.which_children(pool_sup), peername1, 0)
    end)

    # The cluster starts a pool to the other node.
    assert_pool_started(test_ref, host2)

    assert %{pools: %{^peername2 => pool2}} = :sys.get_state(cluster)

    # StatusChange UP, which doesn't change which host goes up.
    send(cluster, {:host_up, host1})

    TestHelper.wait_for_passing(100, fn ->
      assert :sys.get_state(cluster).pools == %{peername2 => pool2}
    end)
  end

  test "handles topology change events", %{test_ref: test_ref} do
    host = %Host{address: {199, 0, 0, 1}, port: 9042}
    new_host = %Host{address: {199, 0, 0, 2}, port: 9042}
    ignored_host = %Host{address: {199, 0, 0, 3}, port: 9042}

    opts = [
      xandra_module: PoolMock,
      control_connection_module: ControlConnectionMock,
      nodes: ["node1"],
      target_pools: 2,
      load_balancing_policy: {LoadBalancingPolicy.DCAwareRoundRobin, []}
    ]

    cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

    assert_control_connection_started(test_ref)

    discovered_peers(cluster, [host])
    assert_pool_started(test_ref, host)

    # TopologyChange NEW_NODE starts a new pool to the node.
    send(cluster, {:host_added, new_host})
    assert_pool_started(test_ref, new_host)

    # TopologyChange NEW_NODE doesn't do anything, since we reached :target_pools.
    send(cluster, {:host_added, ignored_host})
    refute_other_pools_started(test_ref)
    send(cluster, {:host_removed, ignored_host})

    # TopologyChange REMOVED_NODE (removing the original node) stops the pool.
    assert {:ok, pool} = Map.fetch(:sys.get_state(cluster).pools, Host.to_peername(host))
    pool_monitor_ref = Process.monitor(pool)
    send(cluster, {:host_removed, host})
    assert_receive {:DOWN, ^pool_monitor_ref, _, _, _}

    TestHelper.wait_for_passing(500, fn ->
      assert [_] = Supervisor.which_children(:sys.get_state(cluster).pool_supervisor)
    end)
  end

  test "handles the same peers being re-reported", %{test_ref: test_ref} do
    host1 = %Host{address: {192, 0, 0, 1}, port: 9042}
    host2 = %Host{address: {192, 0, 0, 2}, port: 9042}

    opts = [
      xandra_module: PoolMock,
      control_connection_module: ControlConnectionMock,
      nodes: [Host.format_address(host1)]
    ]

    # Start the cluster.
    cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

    # First, the control connection is started and both pools as well.
    assert_control_connection_started(test_ref)
    discovered_peers(cluster, [host1])
    assert_pool_started(test_ref, host1)
    assert Map.has_key?(:sys.get_state(cluster).pools, Host.to_peername(host1))

    # Now simulate the control connection re-reporting different peers for some reason.
    discovered_peers(cluster, [host1, host2])

    assert_pool_started(test_ref, host2)
    refute_other_pools_started(test_ref)

    assert Map.has_key?(:sys.get_state(cluster).pools, Host.to_peername(host2))
  end

  describe "checkout call" do
    test "with no pools" do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1"]
      ]

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert GenServer.call(cluster, :checkout) == {:error, :empty}
    end

    test "with the Random load-balancing policy", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1", "node2"],
        load_balancing: {LoadBalancingPolicy.Random, []}
      ]

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert_control_connection_started(test_ref)

      discovered_peers(cluster, [
        host1 = %Host{address: {192, 0, 0, 1}, port: 9042},
        host2 = %Host{address: {192, 0, 0, 2}, port: 9042}
      ])

      assert_pool_started(test_ref, host1)
      assert_pool_started(test_ref, host2)

      lbs = :sys.get_state(cluster) |> Map.get(:load_balancing_state)
      assert MapSet.new(lbs) == MapSet.new([{host1, :up}, {host2, :up}])

      connected_peers(cluster, [host1, host2])

      lbs = :sys.get_state(cluster) |> Map.get(:load_balancing_state)
      assert MapSet.new(lbs) == MapSet.new([{host1, :connected}, {host2, :connected}])

      pool_pids =
        TestHelper.wait_for_passing(500, fn ->
          pools = :sys.get_state(cluster).pools
          assert map_size(pools) == 2
          for {_address, pid} <- pools, do: pid
        end)

      # If we check out enough times with load_balancing: :random, statistically it
      # means we have to have a non-sorted list of pids.
      random_pids =
        for _ <- 1..50 do
          assert {:ok, pid} = GenServer.call(cluster, :checkout)
          assert pid in pool_pids
          pid
        end

      assert random_pids != Enum.sort(random_pids, :asc) and
               random_pids != Enum.sort(random_pids, :desc)
    end

    @tag skip: "a proper :priority strategy has not been implemented yet"
    test "with load balancing :priority", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1", "node2"],
        load_balancing: :priority
      ]

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert_control_connection_started(test_ref)

      discovered_peers(cluster, [
        host1 = %Host{address: {192, 0, 0, 1}, port: 9042},
        host2 = %Host{address: {192, 0, 0, 2}, port: 9042}
      ])

      assert_pool_started(test_ref, host1)
      assert_pool_started(test_ref, host2)

      %{{{192, 0, 0, 1}, 9042} => pid1, {{192, 0, 0, 2}, 9042} => pid2} =
        TestHelper.wait_for_passing(500, fn ->
          pools = :sys.get_state(cluster).pools
          assert map_size(pools) == 2
          pools
        end)

      assert GenServer.call(cluster, :checkout) == {:ok, pid1}

      # StatusChange DOWN to bring the connection to the first node down, so that :priority
      # selects the second one the next time.

      pool_monitor_ref = Process.monitor(pid1)
      send(cluster, {:host_down, %Host{address: {192, 0, 0, 1}, port: 9042}})
      assert_receive {:DOWN, ^pool_monitor_ref, _, _, _}

      assert GenServer.call(cluster, :checkout) == {:ok, pid2}
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

  defp assert_pool_started(test_ref, %Host{} = host) do
    node = Host.format_address(host)
    assert_receive {^test_ref, PoolMock, :init_called, %{nodes: [^node]}}
  end

  defp refute_other_pools_started(test_ref) do
    refute_receive {^test_ref, PoolMock, :init_called, _args}, 50
  end

  defp assert_control_connection_started(test_ref) do
    assert_receive {^test_ref, ControlConnectionMock, :init_called, _start_args}
  end

  defp discovered_peers(cluster, hosts) do
    Enum.each(hosts, &send(cluster, {:host_added, &1}))
  end

  defp connected_peers(cluster, hosts) do
    Enum.each(hosts, &send(cluster, {:host_connected, &1}))
  end
end
