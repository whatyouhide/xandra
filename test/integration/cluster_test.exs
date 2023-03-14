defmodule Xandra.ClusterTest do
  use ExUnit.Case

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

  describe "with controlled mock processes" do
    test "starts a single control connection", %{test_ref: test_ref} do
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

    test "starts the pool once the control connection reports as active", %{test_ref: test_ref} do
      {address, port} = {{199, 0, 0, 1}, 9042}

      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1.example.com"]
      ]

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert_receive {^test_ref, ControlConnectionMock, :init_called, control_conn_args}
      assert control_conn_args[:contact_points] == [{'node1.example.com', 9042}]

      discovered_peers(cluster, [%Host{address: address, port: port}])
      assert_pool_started(test_ref, {address, port})
    end

    test "starts one pool per node", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1.example.com", "node2.example.com"]
      ]

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert_receive {^test_ref, ControlConnectionMock, :init_called, args}
      assert args.contact_points == [{'node1.example.com', 9042}, {'node2.example.com', 9042}]

      discovered_peers(cluster, [
        %Host{address: {199, 0, 0, 1}, port: 9042},
        %Host{address: {199, 0, 0, 10}, port: 9042}
      ])

      # Assert that the cluster starts a pool for each discovered peer.
      assert_pool_started(test_ref, "199.0.0.1:9042")
      assert_pool_started(test_ref, "199.0.0.10:9042")
    end
  end

  test "handles status change events", %{test_ref: test_ref} do
    peername = {address, port} = {{199, 0, 0, 1}, 9042}

    opts = [
      xandra_module: PoolMock,
      control_connection_module: ControlConnectionMock,
      nodes: ["node1"]
    ]

    cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

    assert_receive {^test_ref, ControlConnectionMock, :init_called, _args}

    discovered_peers(cluster, [%Host{address: address, port: port}])
    assert_pool_started(test_ref, peername)

    %{pool_supervisor: pool_sup, pools: %{^peername => pool}} = :sys.get_state(cluster)

    assert [{^peername, ^pool, :worker, _}] = Supervisor.which_children(pool_sup)
    assert Process.alive?(pool)

    pool_monitor_ref = Process.monitor(pool)

    # StatusChange DOWN:
    send(cluster, {:host_down, %Host{address: address, port: 9042}})
    assert_receive {:DOWN, ^pool_monitor_ref, _, _, _}
    assert [{^peername, :undefined, :worker, _}] = Supervisor.which_children(pool_sup)

    assert :sys.get_state(cluster).pools == %{}

    # StatusChange UP:
    send(cluster, {:host_up, %Host{address: address, port: 9042}})

    new_pool =
      TestHelper.wait_for_passing(500, fn ->
        assert [{^peername, pid, :worker, _}] = Supervisor.which_children(pool_sup)
        assert is_pid(pid)
        pid
      end)

    assert :sys.get_state(cluster).pools == %{peername => new_pool}
  end

  test "handles topology change events", %{test_ref: test_ref} do
    peername = {address, port} = {{199, 0, 0, 1}, 9042}
    new_address = {199, 0, 0, 2}

    opts = [
      xandra_module: PoolMock,
      control_connection_module: ControlConnectionMock,
      nodes: ["node1"]
    ]

    cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

    assert_receive {^test_ref, ControlConnectionMock, :init_called, _args}

    discovered_peers(cluster, [%Host{address: address, port: port}])
    assert_pool_started(test_ref, peername)

    # TopologyChange NEW_NODE

    send(cluster, {:host_added, %Host{address: new_address, port: port}})

    TestHelper.wait_for_passing(500, fn ->
      assert_pool_started(test_ref, {new_address, port})
    end)

    # TopologyChange REMOVED_NODE (removing the original node)

    %{pools: %{^peername => pool}, control_connection: control_conn} = :sys.get_state(cluster)
    assert is_pid(control_conn)

    pool_monitor_ref = Process.monitor(pool)

    send(cluster, {:host_removed, %Host{address: address, port: port}})
    assert_receive {:DOWN, ^pool_monitor_ref, _, _, _}

    TestHelper.wait_for_passing(500, fn ->
      assert [_] = Supervisor.which_children(:sys.get_state(cluster).pool_supervisor)
    end)
  end

  test "handles the same peers being re-reported", %{test_ref: test_ref} do
    # Sometimes, a seed control connection will start up, report active, and report peers before
    # other seed control connections had a chance to report active. This causes us to start
    # control connections to discovered nodes that might match with other seed nodes, effectively
    # starting multiple control connections for the same node.
    # We want to test that the cluster code is able to shut down unnecessary control connections
    # and keep its state clean.

    seed1_ip = {192, 0, 0, 1}
    seed2_ip = {192, 0, 0, 2}
    port = 9042

    opts = [
      xandra_module: PoolMock,
      control_connection_module: ControlConnectionMock,
      nodes: [to_string(:inet.ntoa(seed1_ip))]
    ]

    # Start the cluster.
    cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

    # First, the control connection is started and both pools as well.
    assert_receive {^test_ref, ControlConnectionMock, :init_called, _args}
    discovered_peers(cluster, [%Host{address: seed1_ip, port: port}])
    assert_pool_started(test_ref, {seed1_ip, port})
    assert Map.has_key?(:sys.get_state(cluster).pools, {seed1_ip, port})

    # Now simulate the control connection re-reporting different peers for some reason.
    discovered_peers(cluster, [
      %Host{address: seed1_ip, port: port},
      %Host{address: seed2_ip, port: port}
    ])

    assert_pool_started(test_ref, {seed2_ip, port})
    assert Map.has_key?(:sys.get_state(cluster).pools, {seed2_ip, port})
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

    test "with load balancing :random", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1", "node2"],
        load_balancing: :random
      ]

      cluster = TestHelper.start_link_supervised!({Xandra.Cluster, opts})

      assert_receive {^test_ref, ControlConnectionMock, :init_called, _args}

      discovered_peers(cluster, [
        %Host{address: {192, 0, 0, 1}, port: 9042},
        %Host{address: {192, 0, 0, 2}, port: 9042}
      ])

      assert_pool_started(test_ref, "192.0.0.1:9042")
      assert_pool_started(test_ref, "192.0.0.2:9042")

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

      assert_receive {^test_ref, ControlConnectionMock, :init_called, _args}

      discovered_peers(cluster, [
        %Host{address: {192, 0, 0, 1}, port: 9042},
        %Host{address: {192, 0, 0, 2}, port: 9042}
      ])

      assert_pool_started(test_ref, "192.0.0.1:9042")
      assert_pool_started(test_ref, "192.0.0.2:9042")

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

  defp assert_pool_started(test_ref, node) do
    node =
      case node do
        node when is_binary(node) -> node
        {ip, port} when is_tuple(ip) and port in 0..65535 -> "#{:inet.ntoa(ip)}:#{port}"
      end

    assert_receive {^test_ref, PoolMock, :init_called, %{nodes: [^node]}}
  end

  defp discovered_peers(cluster, hosts) do
    send(cluster, {:discovered_peers, hosts})
  end
end
