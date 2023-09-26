defmodule Xandra.Cluster.PoolTest do
  use ExUnit.Case, async: false

  import Mox

  alias Xandra.Cluster.ControlConnection
  alias Xandra.Cluster.Host
  alias Xandra.Cluster.Pool
  alias Xandra.TestHelper

  defmacrop assert_telemetry(postfix, meta) do
    quote do
      event = [:xandra, :cluster] ++ unquote(postfix)
      telemetry_ref = var!(telemetry_ref)
      assert_receive {^event, ^telemetry_ref, measurements, unquote(meta)}
      assert measurements == %{}
    end
  end

  @protocol_version XandraTest.IntegrationCase.protocol_version()
  @port String.to_integer(System.get_env("CASSANDRA_PORT", "9052"))

  setup :set_mox_from_context
  setup :verify_on_exit!

  setup do
    base_cluster_options = [
      control_connection_module: ControlConnection,
      nodes: [{~c"127.0.0.1", @port}],
      load_balancing: :random,
      autodiscovered_nodes_port: @port,
      xandra_module: Xandra,
      target_pools: 2,
      sync_connect: false,
      refresh_topology_interval: 60_000,
      queue_before_connecting: [
        buffer_size: 100,
        timeout: 5000
      ]
    ]

    pool_opts =
      if @protocol_version do
        [protocol_version: @protocol_version]
      else
        []
      end

    %{cluster_options: base_cluster_options, pool_options: pool_opts}
  end

  describe "startup" do
    test "doesn't fail to start if the control connection fails to connect",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      cluster_opts = Keyword.put(cluster_options, :nodes, [{~c"127.0.0.1", _bad_port = 8092}])
      assert {:ok, pid} = start_supervised(spec(cluster_opts, pool_options))
      :sys.get_state(pid)
    end

    test "tries all the nodes for the control connection (asking the load-balancing policy first)",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :cluster, :control_connection, :failed_to_connect]
        ])

      LBPMock
      |> expect(:init, fn _pid -> nil end)
      |> expect(:query_plan, fn nil -> {[%Host{address: ~c"127.0.0.1", port: 8091}], nil} end)

      cluster_opts =
        Keyword.merge(cluster_options,
          nodes: [
            {~c"127.0.0.1", _bad_port = 8092},
            {~c"127.0.0.1", _bad_port = 8093},
            {~c"127.0.0.1", _bad_port = 8094}
          ],
          load_balancing: {LBPMock, self()}
        )

      assert {:ok, pid} = start_supervised(spec(cluster_opts, pool_options))

      # First, the host returned by the LBP.
      assert_receive {[:xandra, :cluster, :control_connection, :failed_to_connect],
                      ^telemetry_ref, %{},
                      %{
                        reason: :econnrefused,
                        cluster_pid: ^pid,
                        host: %Host{address: {127, 0, 0, 1}, port: 8091}
                      }}

      # Then, the contact nodes.
      for port <- [8092, 8093, 8094] do
        assert_receive {[:xandra, :cluster, :control_connection, :failed_to_connect],
                        ^telemetry_ref, %{},
                        %{
                          reason: :econnrefused,
                          cluster_pid: ^pid,
                          host: %Host{address: {127, 0, 0, 1}, port: ^port}
                        }}
      end
    end

    test "establishes a control connection and a pool",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :cluster, :control_connection, :connected],
          [:xandra, :cluster, :pool, :started],
          [:xandra, :cluster, :change_event],
          [:xandra, :connected]
        ])

      pid = TestHelper.start_link_supervised!(spec(cluster_options, pool_options))

      assert_telemetry [:control_connection, :connected], %{
        cluster_pid: ^pid,
        host: %Host{address: {127, 0, 0, 1}, port: @port}
      }

      assert_telemetry [:pool, :started], %{
        cluster_pid: ^pid,
        host: %Host{address: {127, 0, 0, 1}, port: @port}
      }

      assert_telemetry [:change_event], %{
        cluster_pid: ^pid,
        event_type: :host_added,
        host: %Host{address: {127, 0, 0, 1}, port: @port}
      }

      assert_receive {[:xandra, :connected], ^telemetry_ref, %{},
                      %{address: ~c"127.0.0.1", port: @port}}

      cluster_state = get_state(pid)
      assert map_size(cluster_state.peers) == 1

      assert %{status: status, pool_pid: pool_pid, host: host} =
               cluster_state.peers[{{127, 0, 0, 1}, @port}]

      # Let's avoid race conditions...
      assert status in [:up, :connected]
      assert is_pid(pool_pid)
      assert %Host{address: {127, 0, 0, 1}, port: @port, data_center: "datacenter1"} = host
    end

    @tag :capture_log
    test "starts a pool to a node that is reported in the cluster, but is not up",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :cluster, :control_connection, :connected],
          [:xandra, :cluster, :pool, :started],
          [:xandra, :cluster, :pool, :stopped],
          [:xandra, :cluster, :change_event],
          [:xandra, :connected],
          [:xandra, :failed_to_connect]
        ])

      assert {:ok, pid} = start_supervised(spec(cluster_options, pool_options))

      assert_telemetry [:change_event], %{
        event_type: :host_added,
        cluster_pid: ^pid,
        host: %Host{address: {127, 0, 0, 1}, port: @port} = existing_host
      }

      bad_host = %Host{address: {127, 0, 0, 1}, port: 8092}
      send(pid, {:discovered_hosts, [existing_host, bad_host]})

      assert_telemetry [:change_event], %{
        event_type: :host_added,
        cluster_pid: ^pid,
        host: ^bad_host
      }

      assert_receive {[:xandra, :failed_to_connect], ^telemetry_ref, %{},
                      %{address: ~c"127.0.0.1", port: 8092}}

      assert_telemetry [:pool, :stopped], %{
        cluster_pid: ^pid,
        host: %Host{address: {127, 0, 0, 1}, port: 8092}
      }

      cluster_state = get_state(pid)
      assert %{status: :down} = cluster_state.peers[{{127, 0, 0, 1}, 8092}]
    end

    test "waits for nodes to be up with :sync_connect",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:xandra, :connected]])

      cluster_options = Keyword.merge(cluster_options, sync_connect: 1000)
      assert {:ok, _pid} = start_supervised(spec(cluster_options, pool_options))

      # Assert that we already received events.
      assert_received {[:xandra, :connected], ^telemetry_ref, %{}, %{}}
    end

    test "times out correctly with :sync_connect",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      cluster_options = Keyword.merge(cluster_options, sync_connect: 0)
      assert {:error, :sync_connect_timeout} = Pool.start_link(cluster_options, pool_options)
    end
  end

  describe "handling change events" do
    test ":host_down followed by :host_up", %{
      cluster_options: cluster_opts,
      pool_options: pool_opts
    } do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :cluster, :pool, :started],
          [:xandra, :cluster, :pool, :stopped],
          [:xandra, :cluster, :pool, :restarted]
        ])

      host = %Host{address: {127, 0, 0, 1}, port: @port}

      assert {:ok, pid} = start_supervised(spec(cluster_opts, pool_opts))

      assert_receive {[:xandra, :cluster, :pool, :started], ^telemetry_ref, %{}, %{}}

      # Send the DOWN event.
      send(pid, {:host_down, host.address, host.port})

      assert_receive {[:xandra, :cluster, :pool, :stopped], ^telemetry_ref, %{}, meta}
      assert %Host{address: {127, 0, 0, 1}, port: @port} = meta.host

      assert %{status: :down, pool_pid: nil, host: %Host{address: {127, 0, 0, 1}, port: @port}} =
               get_state(pid).peers[Host.to_peername(host)]

      # Send the UP event.
      send(pid, {:host_up, host.address, host.port})

      assert_receive {[:xandra, :cluster, :pool, :restarted], ^telemetry_ref, %{}, meta}
      assert %Host{address: {127, 0, 0, 1}, port: @port} = meta.host

      assert %{
               status: :up,
               pool_pid: pool_pid,
               host: %Host{address: {127, 0, 0, 1}, port: @port}
             } = get_state(pid).peers[Host.to_peername(host)]

      assert is_pid(pool_pid)
    end

    test "multiple :discovered_hosts where hosts are removed",
         %{cluster_options: cluster_opts, pool_options: pool_opts} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :cluster, :pool, :started],
          [:xandra, :cluster, :pool, :stopped],
          [:xandra, :cluster, :pool, :restarted],
          [:xandra, :cluster, :change_event]
        ])

      good_host = %Host{address: {127, 0, 0, 1}, port: @port}
      bad_host = %Host{address: {127, 0, 0, 1}, port: 8092}

      assert {:ok, pid} = start_supervised(spec(cluster_opts, pool_opts))

      assert_receive {[:xandra, :cluster, :pool, :started], ^telemetry_ref, %{}, %{}}

      assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                      %{event_type: :host_added}}

      send(pid, {:discovered_hosts, [good_host, bad_host]})

      assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                      %{
                        event_type: :host_added,
                        host: %Host{address: {127, 0, 0, 1}, port: 8092}
                      }}

      assert_receive {[:xandra, :cluster, :pool, :started], ^telemetry_ref, %{},
                      %{host: %Host{address: {127, 0, 0, 1}, port: 8092}}}

      # Now remove the bad host.
      send(pid, {:discovered_hosts, [good_host]})

      assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                      %{event_type: :host_removed, host: ^bad_host}}

      assert_receive {[:xandra, :cluster, :pool, :stopped], ^telemetry_ref, %{},
                      %{host: %Host{address: {127, 0, 0, 1}, port: 8092}}}

      assert get_state(pid).pool_supervisor
             |> Supervisor.which_children()
             |> List.keyfind({{127, 0, 0, 1}, 8092}, 0) == nil
    end
  end

  describe "checkout" do
    test "returns the right pool",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      assert {:ok, pid} = start_supervised(spec(cluster_options, pool_options))

      assert {:ok, [{pool_pid, %Host{}}]} = Pool.checkout(pid)
      assert is_pid(pool_pid)
    end

    test "returns all connected pools",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      assert {:ok, pid} = start_supervised(spec(cluster_options, pool_options))

      hosts_with_statuses = [
        {%Host{address: {127, 0, 0, 1}, port: 8091}, :up},
        {%Host{address: {127, 0, 0, 1}, port: 8092}, :down},
        {%Host{address: {127, 0, 0, 1}, port: 8093}, :connected}
      ]

      wait_until_connected(pid)
      send(pid, {:add_test_hosts, hosts_with_statuses})

      assert {:ok, pids_with_hosts} = Pool.checkout(pid)
      assert is_list(pids_with_hosts)

      assert Enum.all?(pids_with_hosts, fn {conn, %Host{}} -> is_pid(conn) end)

      expected_set_of_connected_hosts =
        MapSet.new([{{127, 0, 0, 1}, @port}, {{127, 0, 0, 1}, 8093}])

      existing_set_of_connected_hosts =
        MapSet.new(pids_with_hosts, fn {_, host} -> Host.to_peername(host) end)

      assert existing_set_of_connected_hosts == expected_set_of_connected_hosts
    end

    test "returns {:error, :empty} when there are no pools",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      cluster_options =
        Keyword.merge(cluster_options,
          nodes: [{~c"127.0.0.1", 8092}],
          queue_before_connecting: [timeout: 0, buffer_size: 0]
        )

      assert {:ok, pid} = start_supervised(spec(cluster_options, pool_options))

      assert {:error, :empty} = Pool.checkout(pid)
    end
  end

  describe "resiliency" do
    test "if a connection pool crashes, the pool process stays up and cleans up",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      cluster_options = Keyword.merge(cluster_options, sync_connect: 1000)
      cluster = start_supervised!(spec(cluster_options, pool_options))

      assert {:ok, [{pool_pid, %Host{}}]} = Pool.checkout(cluster)
      ref = Process.monitor(pool_pid)

      Process.exit(pool_pid, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}

      assert %{status: :up, pool_pid: nil, pool_ref: nil} =
               get_state(cluster).peers[{{127, 0, 0, 1}, @port}]
    end

    test "if the connection pool supervisor crashes, the pool crashes as well",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      # Don't *link* the cluster to the test process, to avoid having to trap exits.
      cluster_options = Keyword.merge(cluster_options, sync_connect: 1000)
      cluster = start_supervised!(spec(cluster_options, pool_options))
      cluster_ref = Process.monitor(cluster)

      pool_sup = get_state(cluster).pool_supervisor
      ref = Process.monitor(pool_sup)
      Process.exit(pool_sup, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}

      assert_receive {:DOWN, ^cluster_ref, _, _, :killed}
    end

    @tag :capture_log
    test "if the control connection crashes, the pool crashes as well",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      # Don't *link* the cluster to the test process, to avoid having to trap exits.
      cluster_options = Keyword.merge(cluster_options, sync_connect: 1000)
      cluster = start_supervised!(spec(cluster_options, pool_options))
      cluster_ref = Process.monitor(cluster)

      control_conn = get_state(cluster).control_connection
      control_conn_ref = Process.monitor(control_conn)
      Process.exit(control_conn, :kill)
      assert_receive {:DOWN, ^control_conn_ref, _, _, _}

      assert_receive {:DOWN, ^cluster_ref, _, _, :killed}
    end

    @tag :capture_log
    test "if the control connection shuts down with :closed, the pool is fine and starts a new one",
         %{cluster_options: cluster_options, pool_options: pool_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :cluster, :control_connection, :connected],
          [:xandra, :cluster, :control_connection, :disconnected]
        ])

      cluster_options = Keyword.merge(cluster_options, sync_connect: 1000)
      cluster = TestHelper.start_link_supervised!(spec(cluster_options, pool_options))

      assert_telemetry [:control_connection, :connected], _meta

      control_conn = get_state(cluster).control_connection
      control_conn_ref = Process.monitor(control_conn)

      :ok = :gen_tcp.shutdown(:sys.get_state(control_conn).transport.socket, :read_write)
      assert_telemetry [:control_connection, :disconnected], _meta
      assert_receive {:DOWN, ^control_conn_ref, _, _, _}

      # Make sure we reconnect to the control connection.
      assert_telemetry [:control_connection, :connected], _meta
    end
  end

  defp wait_until_connected(pid, retries \\ 10)

  defp wait_until_connected(_pid, 0), do: :error

  defp wait_until_connected(pid, retries) do
    case :sys.get_state(pid) do
      {:has_connected_once, _} ->
        :ok

      _ ->
        Process.sleep(10)
        wait_until_connected(pid, retries - 1)
    end
  end

  defp spec(cluster_opts, pool_opts) do
    %{
      id: Pool,
      start: {Pool, :start_link, [cluster_opts, pool_opts]}
    }
  end

  defp get_state(cluster) do
    assert {_state, data} = :sys.get_state(cluster)
    data
  end
end
