defmodule Xandra.RetryStrategiesTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.Cluster.Host
  alias Xandra.Error

  describe "on single connection level level" do
    test "calls the c:retry/3 until query succeeds", %{conn: conn} do
      defmodule CounterStrategy do
        @behaviour Xandra.RetryStrategy

        @impl true
        def new(options), do: Keyword.fetch!(options, :retry_count)

        @impl true
        def retry(_error, _options, 0) do
          :error
        end

        def retry(error, options, retries_left) do
          send(self(), {:retrying, error, retries_left})
          {:retry, options, retries_left - 1}
        end
      end

      assert_raise KeyError, ~r"key :retry_count not found", fn ->
        Xandra.execute(conn, "USE nonexistent_keyspace", [], retry_strategy: CounterStrategy)
      end

      assert {:error, %Error{} = error} =
               Xandra.execute(conn, "USE nonexistent_keyspace", [],
                 retry_strategy: CounterStrategy,
                 retry_count: 2
               )

      assert error.reason == :invalid
    after
      :code.delete(CounterStrategy)
      :code.purge(CounterStrategy)
    end

    test "raises an error if c:retry/3 returns an invalid value", %{conn: conn} do
      defmodule InvalidStrategy do
        @behaviour Xandra.RetryStrategy

        @impl true
        def new(_options), do: %{}

        @impl true
        def retry(_error, _options, _state), do: :invalid_value
      end

      message = """
      invalid return value from retry strategy callback \
      Xandra.RetryStrategiesTest.InvalidStrategy.retry/3 with state %{}: :invalid_value\
      """

      assert_raise ArgumentError, message, fn ->
        Xandra.execute(conn, "USE nonexistent_keyspace", [], retry_strategy: InvalidStrategy)
      end
    after
      :code.delete(InvalidStrategy)
      :code.purge(InvalidStrategy)
    end
  end

  describe "on cluster level" do
    defmodule MockXandra do
      use GenServer

      def start_link(opts) do
        GenServer.start_link(__MODULE__, opts)
      end

      @impl true
      def init(opts) do
        {:ok, opts}
      end

      @impl true
      def handle_info(info, state) do
        send(state.conn, info)
        send(state.client, {:received_request, self()})
        {:noreply, state}
      end
    end

    test "works with target_connection", %{start_options: start_options} do
      defmodule AllNodesStrategy do
        @behaviour Xandra.RetryStrategy

        @impl true
        def new(options) do
          send(:all_nodes_strategy_test_pid, {:new_called, options})
          Keyword.fetch!(options, :connected_hosts)
        end

        @impl true
        def retry(_error, options, [{conn_pid, _host} | rest_of_nodes] = state) do
          send(:all_nodes_strategy_test_pid, {:retry_called, state})
          {:retry, options, rest_of_nodes, conn_pid}
        end

        def retry(_error, _options, []) do
          send(:all_nodes_strategy_test_pid, {:retry_called, []})
          :error
        end
      end

      Process.register(self(), :all_nodes_strategy_test_pid)

      start_options = Keyword.merge(start_options, sync_connect: 1000)
      cluster = start_supervised!({Xandra.Cluster, start_options})

      assert {:error, %Xandra.Error{reason: :invalid}} =
               Xandra.Cluster.execute(cluster, "USE nonexistent_keyspace", [],
                 retry_strategy: AllNodesStrategy
               )

      assert_received {:new_called, options}
      assert [{conn_pid, %Host{}}] = options[:connected_hosts]

      assert_received {:retry_called, [{^conn_pid, %Host{}}]}
      assert_received {:retry_called, []}
    after
      :code.delete(AllNodesStrategy)
      :code.purge(AllNodesStrategy)
    end

    test "raises an error if c:retry/3 returns an invalid value", %{conn: conn} do
      defmodule InvalidClusterStrategy do
        @behaviour Xandra.RetryStrategy

        @impl true
        def new(_options), do: %{}

        @impl true
        def retry(_error, _options, _state), do: :invalid_value
      end

      message = """
      invalid return value from retry strategy callback \
      Xandra.RetryStrategiesTest.InvalidClusterStrategy.retry/3 with state %{}: :invalid_value\
      """

      assert_raise ArgumentError, message, fn ->
        Xandra.execute(conn, "USE nonexistent_keyspace", [],
          retry_strategy: InvalidClusterStrategy
        )
      end
    after
      :code.delete(InvalidClusterStrategy)
      :code.purge(InvalidClusterStrategy)
    end
  end
end
