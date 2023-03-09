defmodule Xandra.ClusterTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Xandra.Cluster.{StatusChange, TopologyChange}

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
      message =
        ~s(list element at position 0 in :nodes failed validation: invalid node: "foo:bar")

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["foo:bar"])
      end

      message =
        ~s(list element at position 0 in :nodes failed validation: invalid node: "example.com:9042x")

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042x"])
      end
    end

    test "validates the :autodiscovery option" do
      message = ~r/expected :autodiscovery to be a boolean/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], autodiscovery: "not a boolean")
      end
    end

    test "validates the :autodiscovered_nodes_port option" do
      message = ~r/expected :autodiscovered_nodes_port to be in 0\.\.65535/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], autodiscovered_nodes_port: 99_999)
      end
    end

    test "validates the :load_balancing option" do
      message = ~r/expected :load_balancing to be in \[:priority, :random\]/

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
    test "starts one control connection per node for the given nodes", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1.example.com", "node2.example.com"]
      ]

      cluster = start_supervised!({Xandra.Cluster, opts})

      assert_receive {^test_ref, ControlConnectionMock, :init_called,
                      %{address: 'node1.example.com'} = args}

      assert is_reference(args.node_ref)
      assert args.cluster == cluster

      assert_receive {^test_ref, ControlConnectionMock, :init_called,
                      %{address: 'node2.example.com'} = args}

      assert is_reference(args.node_ref)
      assert args.cluster == cluster
    end

    test "starts the pool once the control connection reports as active", %{test_ref: test_ref} do
      {address, port} = {{199, 0, 0, 1}, 9042}

      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1.example.com"],
        autodiscovery: false
      ]

      cluster = start_supervised!({Xandra.Cluster, opts})

      assert_receive {^test_ref, ControlConnectionMock, :init_called, control_conn_args}
      assert control_conn_args[:address] == 'node1.example.com'

      assert :ok = Xandra.Cluster.activate(cluster, control_conn_args[:node_ref], {address, port})

      assert_pool_started(test_ref, {address, port})
    end

    test "starts one control connection per node including discovered nodes", %{
      test_ref: test_ref
    } do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1.example.com", "node2.example.com"],
        autodiscovery: true
      ]

      cluster = start_supervised!({Xandra.Cluster, opts})

      assert_receive {^test_ref, ControlConnectionMock, :init_called,
                      %{address: 'node1.example.com'} = args1}

      assert_receive {^test_ref, ControlConnectionMock, :init_called,
                      %{address: 'node2.example.com'} = args2}

      # Simulate the control connection going up and reporting peers right away.
      assert :ok = Xandra.Cluster.activate(cluster, args1.node_ref, {{199, 0, 0, 1}, 9042})
      assert :ok = Xandra.Cluster.discovered_peers(cluster, [{199, 0, 0, 10}], "199.0.0.1")

      # Assert that the cluster starts a pool for the node going up.
      assert_pool_started(test_ref, "199.0.0.1:9042")

      # Assert that the cluster starts a control connection for the discovered peer.
      assert_receive {^test_ref, ControlConnectionMock, :init_called,
                      %{address: {199, 0, 0, 10}, node_ref: discovered_node_ref}}

      assert :ok = Xandra.Cluster.activate(cluster, discovered_node_ref, {{199, 0, 0, 10}, 9042})
      assert_pool_started(test_ref, "199.0.0.10:9042")

      # Now some fun: simulate that the control connection for the second "seed" node goes up and
      # reports two peers: a new one and a one that was already reported.
      assert :ok = Xandra.Cluster.activate(cluster, args2.node_ref, {{199, 0, 0, 2}, 9042})

      # Assert that the cluster starts a pool for the node going up.
      assert_pool_started(test_ref, "199.0.0.2:9042")

      assert :ok =
               Xandra.Cluster.discovered_peers(
                 cluster,
                 [{199, 0, 0, 11}, {199, 0, 0, 10}],
                 "199.0.0.2"
               )

      # Assert that the control connection goes up for the new peer.
      assert_receive {^test_ref, ControlConnectionMock, :init_called, %{address: {199, 0, 0, 11}}}

      # Make sure that the control connection does not go up for the already-discovered peer.
      refute_receive {^test_ref, ControlConnectionMock, :init_called, _args}
    end
  end

  test "handles status change events", %{test_ref: test_ref} do
    peername = {address, port} = {{199, 0, 0, 1}, 9042}

    opts = [
      xandra_module: PoolMock,
      control_connection_module: ControlConnectionMock,
      nodes: ["node1"],
      autodiscovery: false
    ]

    cluster = start_supervised!({Xandra.Cluster, opts})

    assert_receive {^test_ref, ControlConnectionMock, :init_called,
                    %{address: 'node1', node_ref: ref}}

    assert :ok = Xandra.Cluster.activate(cluster, ref, {address, port})
    assert_pool_started(test_ref, peername)
    %{pool_supervisor: pool_sup, pools: %{^peername => pool}} = :sys.get_state(cluster)

    assert [{^peername, ^pool, :worker, _}] = Supervisor.which_children(pool_sup)
    assert Process.alive?(pool)

    pool_monitor_ref = Process.monitor(pool)

    # StatusChange DOWN:

    assert :ok = Xandra.Cluster.update(cluster, %StatusChange{effect: "DOWN", address: address})

    assert_receive {:DOWN, ^pool_monitor_ref, _, _, _}

    assert [{^peername, :undefined, :worker, _}] = Supervisor.which_children(pool_sup)

    assert :sys.get_state(cluster).pools == %{}

    # StatusChange UP:

    assert :ok = Xandra.Cluster.update(cluster, %StatusChange{effect: "UP", address: address})

    new_pool =
      wait_for_passing(200, fn ->
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
      nodes: ["node1"],
      autodiscovery: true
    ]

    cluster = start_supervised!({Xandra.Cluster, opts})

    assert_receive {^test_ref, ControlConnectionMock, :init_called,
                    %{address: 'node1', node_ref: ref}}

    assert :ok = Xandra.Cluster.activate(cluster, ref, {address, port})
    assert_pool_started(test_ref, peername)

    # TopologyChange NEW_NODE

    assert :ok =
             Xandra.Cluster.update(cluster, %TopologyChange{
               effect: "NEW_NODE",
               address: new_address
             })

    assert_receive {^test_ref, ControlConnectionMock, :init_called,
                    %{address: ^new_address, node_ref: new_ref}}

    assert :ok = Xandra.Cluster.activate(cluster, new_ref, {new_address, 9042})
    assert_pool_started(test_ref, {new_address, port})

    # TopologyChange REMOVED_NODE
    # (removing the original node)

    %{pools: %{^peername => pool}, control_conn_supervisor: control_conn_sup} =
      :sys.get_state(cluster)

    control_conn =
      Enum.find_value(Supervisor.which_children(control_conn_sup), fn
        {^ref, pid, _, _} -> pid
        _other -> nil
      end)

    assert is_pid(control_conn)

    pool_monitor_ref = Process.monitor(pool)
    control_conn_monitor_ref = Process.monitor(control_conn)

    assert :ok =
             Xandra.Cluster.update(cluster, %TopologyChange{
               effect: "REMOVED_NODE",
               address: address
             })

    assert_receive {:DOWN, ^pool_monitor_ref, _, _, _}
    assert_receive {:DOWN, ^control_conn_monitor_ref, _, _, _}

    wait_for_passing(500, fn ->
      assert [_] = Supervisor.which_children(:sys.get_state(cluster).pool_supervisor)
      assert [_] = Supervisor.which_children(:sys.get_state(cluster).control_conn_supervisor)
    end)
  end

  test "ignores topology change MOVED_NODE events" do
    opts = [
      xandra_module: PoolMock,
      control_connection_module: ControlConnectionMock,
      nodes: ["node1"],
      autodiscovery: true
    ]

    cluster = start_supervised!({Xandra.Cluster, opts})

    # TopologyChange MOVED_NODE

    assert capture_log(fn ->
             assert :ok =
                      Xandra.Cluster.update(cluster, %TopologyChange{
                        effect: "MOVED_NODE",
                        address: {192, 0, 0, 1}
                      })

             # We have to fall back to sleeping here because we cannot use wait_for_passing/2: we
             # don't know when to stop capturing the log.
             Process.sleep(100)
           end) =~ "Ignored TOPOLOGY_CHANGE event"
  end

  test "handles race conditions for node discovery", %{test_ref: test_ref} do
    # Sometimes, a seed control connection will start up, report active, and report peers before
    # other seed control connections had a chance to report active. This causes us to start
    # control connections to discovered nodes that might match with other seed nodes, effectively
    # starting multiple control connections for the same node.
    # We want to test that the cluster code is able to shut down unnecessary control connections
    # and keep its state clean.

    seed1_ip = {192, 0, 0, 1}
    seed2_ip = {192, 0, 0, 2}
    seed1_ip_charlist = :inet.ntoa(seed1_ip)
    seed2_ip_charlist = :inet.ntoa(seed2_ip)
    port = 9042

    opts = [
      xandra_module: PoolMock,
      control_connection_module: ControlConnectionMock,
      nodes: [to_string(seed1_ip_charlist), to_string(seed2_ip_charlist)],
      autodiscovery: true
    ]

    # Start the cluster.
    cluster = start_supervised!({Xandra.Cluster, opts})

    # First, both control connections are started.

    assert_receive {^test_ref, ControlConnectionMock, :init_called,
                    %{address: ^seed1_ip_charlist, node_ref: seed1_cc_ref}}

    assert_receive {^test_ref, ControlConnectionMock, :init_called,
                    %{address: ^seed2_ip_charlist, node_ref: seed2_cc_ref}}

    # Let's say only the first one activates (and its pool starts up too).
    assert :ok = Xandra.Cluster.activate(cluster, seed1_cc_ref, {seed1_ip, port})
    assert_pool_started(test_ref, {seed1_ip, port})

    assert :sys.get_state(cluster).node_refs == [
             {:node_ref, seed1_cc_ref, {seed1_ip, port}},
             {:node_ref, seed2_cc_ref, nil}
           ]

    assert Map.has_key?(:sys.get_state(cluster).pools, {seed1_ip, port})

    # Okay cool, now let's have the first seed control connection report the second seed control
    # connection as a peer. Mind = blown?
    assert :ok =
             Xandra.Cluster.discovered_peers(cluster, [seed2_ip], to_string(:inet.ntoa(seed1_ip)))

    # Well, the control connection for seed2 hasn't reported active yet, so the cluster
    # starts a new one.
    assert_receive {^test_ref, ControlConnectionMock, :init_called,
                    %{address: ^seed2_ip, node_ref: seed2_peer_cc_ref}}

    assert :sys.get_state(cluster).node_refs == [
             {:node_ref, seed1_cc_ref, {seed1_ip, port}},
             {:node_ref, seed2_cc_ref, nil},
             {:node_ref, seed2_peer_cc_ref, nil}
           ]

    refute Map.has_key?(:sys.get_state(cluster).pools, {seed2_ip, port})

    # Now the seed2 "peer" control connection reports as active (and we starts its pool).
    assert :ok = Xandra.Cluster.activate(cluster, seed2_peer_cc_ref, {seed2_ip, port})
    assert_pool_started(test_ref, {seed2_ip, port})

    # Ah, now the address <-> node_ref mapping should be updated correctly.
    assert :sys.get_state(cluster).node_refs == [
             {:node_ref, seed1_cc_ref, {seed1_ip, port}},
             {:node_ref, seed2_cc_ref, nil},
             {:node_ref, seed2_peer_cc_ref, {seed2_ip, port}}
           ]

    assert Map.has_key?(:sys.get_state(cluster).pools, {seed2_ip, port})

    # And now the grand finale: the original seed2 control connection reports active (what a slow
    # connection...).
    assert :ok = Xandra.Cluster.activate(cluster, seed2_cc_ref, {seed2_ip, port})

    # Okay, first things first: we should not start a new pool.
    refute_any_pool_started(test_ref)

    # Second things second: the address <-> node_ref mapping should be Good™ and we should have
    # removed the unnecessary control connection.
    assert :sys.get_state(cluster).node_refs == [
             {:node_ref, seed1_cc_ref, {seed1_ip, port}},
             {:node_ref, seed2_peer_cc_ref, {seed2_ip, port}}
           ]

    cc_children = Supervisor.which_children(:sys.get_state(cluster).control_conn_supervisor)

    assert Enum.sort(Enum.map(cc_children, fn {ref, _, _, _} -> ref end)) ==
             Enum.sort([seed1_cc_ref, seed2_peer_cc_ref])
  end

  describe "checkout call" do
    test "with no pools" do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1"],
        autodiscovery: true
      ]

      cluster = start_supervised!({Xandra.Cluster, opts})

      assert GenServer.call(cluster, :checkout) == {:error, :empty}
    end

    test "with load balancing :random", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1", "node2"],
        autodiscovery: false,
        load_balancing: :random
      ]

      cluster = start_supervised!({Xandra.Cluster, opts})

      assert_receive {^test_ref, ControlConnectionMock, :init_called,
                      %{address: 'node1', node_ref: ref1}}

      assert :ok = Xandra.Cluster.activate(cluster, ref1, {{192, 0, 0, 1}, 9042})
      assert_pool_started(test_ref, "192.0.0.1:9042")

      assert_receive {^test_ref, ControlConnectionMock, :init_called,
                      %{address: 'node2', node_ref: ref2}}

      assert :ok = Xandra.Cluster.activate(cluster, ref2, {{192, 0, 0, 2}, 9042})
      assert_pool_started(test_ref, "192.0.0.2:9042")

      pool_pids =
        wait_for_passing(500, fn ->
          pools = :sys.get_state(cluster).pools
          assert map_size(pools) == 2
          for {_address, pid} <- pools, do: pid
        end)

      random_pids =
        for _ <- 1..50 do
          assert {:ok, pid} = GenServer.call(cluster, :checkout)
          assert pid in pool_pids
          pid
        end

      assert random_pids != Enum.sort(random_pids, :asc) and
               random_pids != Enum.sort(random_pids, :desc)
    end

    test "with load balancing :priority", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1", "node2"],
        autodiscovery: false,
        load_balancing: :priority
      ]

      cluster = start_supervised!({Xandra.Cluster, opts})

      assert_receive {^test_ref, ControlConnectionMock, :init_called,
                      %{address: 'node1', node_ref: ref1}}

      assert :ok = Xandra.Cluster.activate(cluster, ref1, {{192, 0, 0, 1}, 9042})
      assert_pool_started(test_ref, "192.0.0.1:9042")

      assert_receive {^test_ref, ControlConnectionMock, :init_called,
                      %{address: 'node2', node_ref: ref2}}

      assert :ok = Xandra.Cluster.activate(cluster, ref2, {{192, 0, 0, 2}, 9042})
      assert_pool_started(test_ref, "192.0.0.2:9042")

      %{{{192, 0, 0, 1}, 9042} => pid1, {{192, 0, 0, 2}, 9042} => pid2} =
        wait_for_passing(500, fn ->
          pools = :sys.get_state(cluster).pools
          assert map_size(pools) == 2
          pools
        end)

      assert GenServer.call(cluster, :checkout) == {:ok, pid1}

      # StatusChange DOWN to bring the connection to the first node down, so that :proriority
      # selects the second one the next time.

      pool_monitor_ref = Process.monitor(pid1)

      assert :ok =
               Xandra.Cluster.update(cluster, %StatusChange{
                 effect: "DOWN",
                 address: {192, 0, 0, 1}
               })

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

  defp refute_any_pool_started(test_ref) do
    refute_receive {^test_ref, PoolMock, :init_called, _args}, 50
  end

  defp wait_for_passing(time_left, fun) when time_left < 0 do
    fun.()
  end

  defp wait_for_passing(time_left, fun) do
    fun.()
  catch
    _, _ ->
      Process.sleep(100)
      wait_for_passing(time_left - 100, fun)
  end
end
