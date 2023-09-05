defmodule Xandra.RetryStrategiesTest do
  use XandraTest.IntegrationCase, async: false

  alias Xandra.Error

  describe "RetryStrategy on Xandra level" do
    test "that retries for a fixed amount of times", %{conn: conn} do
      defmodule CounterStrategy do
        @behaviour Xandra.RetryStrategy

        def new(options) do
          retries_left = Keyword.fetch!(options, :retry_count)

          %{retries_left: retries_left}
        end

        def retry(_error, _options, %{retries_left: 0}) do
          :error
        end

        def retry(error, options, %{retries_left: retries_left}) do
          send(self(), {:retrying, error, retries_left})

          {:retry, options, %{retries_left: retries_left - 1}}
        end
      end

      assert_raise KeyError, fn ->
        Xandra.execute(conn, "USE nonexistent_keyspace", [], retry_strategy: CounterStrategy)
      end

      options = [retry_strategy: CounterStrategy, retry_count: 2]
      assert {:error, _} = Xandra.execute(conn, "USE nonexistent_keyspace", [], options)

      assert_received {:retrying, %Error{reason: :invalid}, 2}
      assert_received {:retrying, %Error{reason: :invalid}, 1}
    after
      :code.delete(CounterStrategy)
      :code.purge(CounterStrategy)
    end

    test "raises an error if retry/3 returns an invalid value", %{conn: conn} do
      defmodule InvalidStrategy do
        @behaviour Xandra.RetryStrategy

        def new(_options), do: %{}
        def retry(_error, _options, _state), do: :invalid_value
      end

      message =
        "invalid return value :invalid_value from " <>
          "retry strategy Xandra.RetryStrategiesTest.InvalidStrategy with state %{}"

      assert_raise ArgumentError, message, fn ->
        Xandra.execute(conn, "USE nonexistend_keyspace", [], retry_strategy: InvalidStrategy)
      end
    after
      :code.delete(InvalidStrategy)
      :code.purge(InvalidStrategy)
    end
  end

  describe "RetryStrategy on cluster level" do
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
        reply = send(state.conn, info)
        send(state.client, {:received_request, self()})
        {:noreply, state}
      end
    end

    defmodule MockCluster do
      @behaviour :gen_statem

      alias Xandra.Cluster.Host

      def child_spec(arg) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [arg]}
        }
      end

      def start_link(nodes_count: nodes_count, conn: conn, client: client) do
        hosts =
          Enum.map(1..nodes_count, fn n ->
            {:ok, xandra_conn} = MockXandra.start_link(%{conn: conn, client: client})
            {xandra_conn, %Host{address: {127, 0, 0, n}}}
          end)

        :gen_statem.start_link(__MODULE__, hosts, [])
      end

      def callback_mode(),
        do: [:handle_event_function]

      def init(hosts) do
        {:ok, :state, hosts}
      end

      def handle_event({:call, from}, :checkout, _state, hosts) do
        {:keep_state_and_data, {:reply, from, {:ok, hosts}}}
      end
    end

    test "works with target_connection", %{conn: conn} do
      defmodule AllNodesStrategy do
        @behaviour Xandra.RetryStrategy

        def new(options) do
          if options[:execution_level] == :cluster do
            [_already_tried_node | rest_of_nodes] = options[:connected_hosts]

            rest_of_nodes
          end
        end

        def retry(_error, options, state) do
          case options[:execution_level] do
            :xandra ->
              :error

            :cluster ->
              case state do
                [] ->
                  :error

                [{conn, _host} | rest_of_nodes] ->
                  options = Keyword.put(options, :target_connection, conn)

                  {:retry, options, rest_of_nodes}
              end
          end
        end
      end

      {:ok, mock_cluster} =
        start_supervised({MockCluster, [nodes_count: 3, conn: conn, client: self()]})

      assert {:ok, [{pid1, _host1}, {pid2, _host2}, {pid3, _host3}] = hosts} =
               Xandra.Cluster.Pool.checkout(mock_cluster)

      assert {:error, %Xandra.Error{reason: :invalid}} =
               Xandra.Cluster.execute(mock_cluster, "USE nonexistent_keyspace", [],
                 retry_strategy: AllNodesStrategy
               )

      assert_receive({:received_request, ^pid1})
      assert_receive({:received_request, ^pid2})
      assert_receive({:received_request, ^pid3})
    end
  end
end
