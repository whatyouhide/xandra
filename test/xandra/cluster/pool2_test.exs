defmodule Xandra.Cluster.Pool2Test do
  use ExUnit.Case, async: false

  import Mox

  alias Xandra.Cluster.ControlConnection2
  alias Xandra.Cluster.Host
  alias Xandra.Cluster.Pool2

  @protocol_version XandraTest.IntegrationCase.protocol_version()
  @port String.to_integer(System.get_env("CASSANDRA_PORT", "9052"))

  setup :set_mox_from_context
  setup :verify_on_exit!

  setup do
    base_start_options = [
      control_connection_module: ControlConnection2,
      nodes: [{~c"127.0.0.1", @port}],
      sync_connect: false,
      registry_listeners: [],
      load_balancing: :random,
      autodiscovered_nodes_port: @port,
      xandra_module: Xandra,
      target_pools: 2,
      refresh_topology_interval: 60_000
    ]

    pool_opts =
      if @protocol_version do
        [protocol_version: @protocol_version]
      else
        []
      end

    %{start_options: base_start_options, pool_options: pool_opts}
  end

  describe "startup" do
    test "doesn't fail to start if the control connection fails to connect",
         %{start_options: start_options, pool_options: pool_options} do
      cluster_opts = Keyword.put(start_options, :nodes, [{~c"127.0.0.1", _bad_port = 8092}])
      assert {:ok, pid} = start_supervised(spec(cluster_opts, pool_options))
      :sys.get_state(pid)
    end

    test "tries all the nodes for the control connection (asking the load-balancing policy first)",
         %{start_options: start_options, pool_options: pool_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :cluster, :control_connection, :failed_to_connect]
        ])

      LBPMock
      |> expect(:init, fn _pid -> nil end)
      |> expect(:query_plan, fn nil -> {[%Host{address: ~c"127.0.0.1", port: 8091}], nil} end)

      cluster_opts =
        Keyword.merge(start_options,
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
                        host: %Host{address: ~c"127.0.0.1", port: 8091}
                      }}

      # Then, the contact nodes.
      for port <- [8092, 8093, 8094] do
        assert_receive {[:xandra, :cluster, :control_connection, :failed_to_connect],
                        ^telemetry_ref, %{},
                        %{
                          reason: :econnrefused,
                          cluster_pid: ^pid,
                          host: %Host{address: ~c"127.0.0.1", port: ^port}
                        }}
      end
    end

    test "establishes a control connection and a pool",
         %{start_options: start_options, pool_options: pool_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :cluster, :control_connection, :connected],
          [:xandra, :cluster, :pool, :started],
          [:xandra, :cluster, :change_event],
          [:xandra, :connected]
        ])

      assert {:ok, pid} = start_supervised(spec(start_options, pool_options))

      assert %{
               cluster_pid: ^pid,
               host: %Host{address: {127, 0, 0, 1}, port: @port}
             } = assert_telemetry(telemetry_ref, [:control_connection, :connected])

      assert %{
               cluster_pid: ^pid,
               host: %Host{address: {127, 0, 0, 1}, port: @port}
             } = assert_telemetry(telemetry_ref, [:pool, :started])

      assert %{
               event_type: :host_added,
               cluster_pid: ^pid,
               host: %Host{address: {127, 0, 0, 1}, port: @port}
             } = assert_telemetry(telemetry_ref, [:change_event])

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

    # TODO: remove this conditional once we depend on Elixir 1.15+, which depends on OTP 24+.
    if System.otp_release() >= "24" do
      test "supports :sync_connect option",
           %{start_options: start_options, pool_options: pool_options} do
        telemetry_ref =
          :telemetry_test.attach_event_handlers(self(), [
            [:xandra, :connected]
          ])

        start_options = Keyword.merge(start_options, sync_connect: 1000)
        start_supervised!(spec(start_options, pool_options))

        # We check that the message was _already_ received here.
        assert_received {[:xandra, :connected], ^telemetry_ref, %{},
                         %{address: ~c"127.0.0.1", port: @port}}
      end
    end

    @tag :capture_log
    test "starts a pool to a node that is reported in the cluster, but is not up",
         %{start_options: start_options, pool_options: pool_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :cluster, :control_connection, :connected],
          [:xandra, :cluster, :pool, :started],
          [:xandra, :cluster, :pool, :stopped],
          [:xandra, :cluster, :change_event],
          [:xandra, :connected],
          [:xandra, :failed_to_connect]
        ])

      assert {:ok, pid} = start_supervised(spec(start_options, pool_options))

      assert %{
               event_type: :host_added,
               cluster_pid: ^pid,
               host: %Host{address: {127, 0, 0, 1}, port: @port} = existing_host
             } = assert_telemetry(telemetry_ref, [:change_event])

      bad_host = %Host{address: {127, 0, 0, 1}, port: 8092}
      send(pid, {:discovered_hosts, [existing_host, bad_host]})

      assert %{
               event_type: :host_added,
               cluster_pid: ^pid,
               host: ^bad_host
             } = assert_telemetry(telemetry_ref, [:change_event])

      assert_receive {[:xandra, :failed_to_connect], ^telemetry_ref, %{},
                      %{address: ~c"127.0.0.1", port: 8092}}

      cluster_state = get_state(pid)
      assert %{status: :down} = cluster_state.peers[{{127, 0, 0, 1}, 8092}]

      assert %{
               cluster_pid: ^pid,
               host: %Host{address: {127, 0, 0, 1}, port: 8092}
             } = assert_telemetry(telemetry_ref, [:pool, :stopped])
    end
  end

  defp spec(cluster_opts, pool_opts) do
    %{
      id: Pool2,
      start: {Pool2, :start_link, [cluster_opts, pool_opts]}
    }
  end

  defp get_state(cluster) do
    assert {:no_state, state} = :sys.get_state(cluster)
    state
  end

  defp assert_telemetry(ref, postfix) do
    event = [:xandra, :cluster] ++ postfix
    assert_receive {^event, ^ref, measurements, metadata}
    assert measurements == %{}
    metadata
  end
end
