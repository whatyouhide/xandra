defmodule Xandra.RetryStrategiesTest do
  use ExUnit.Case, async: false

  alias Xandra.Error

  setup_all do
    options =
      XandraTest.IntegrationCase.default_start_options()
      |> Keyword.put(:sync_connect, 1000)

    conn = start_supervised!({Xandra.Cluster, options})

    %{conn: conn}
  end

  test "strategy that retries for a fixed amount of times", %{conn: conn} do
    defmodule CounterStrategy do
      @behaviour Xandra.RetryStrategy

      def new(connected_hosts, options) do
        retries_left = Keyword.fetch!(options, :retry_count)

        [{conn, _host}] = connected_hosts

        %{conn: conn, retries_left: retries_left}
      end

      def retry(_error, _options, %{retries_left: 0}) do
        :error
      end

      def retry(error, options, %{conn: conn, retries_left: retries_left}) do
        send(self(), {:retrying, error, retries_left})

        {:retry, conn, options, %{conn: conn, retries_left: retries_left - 1}}
      end
    end

    assert_raise KeyError, fn ->
      Xandra.Cluster.execute(conn, "USE nonexistent_keyspace", [],
        retry_strategy: CounterStrategy
      )
    end

    options = [retry_strategy: CounterStrategy, retry_count: 2]
    assert {:error, _} = Xandra.Cluster.execute(conn, "USE nonexistent_keyspace", [], options)

    assert_received {:retrying, %Error{reason: :invalid}, 2}
    assert_received {:retrying, %Error{reason: :invalid}, 1}
  after
    :code.delete(CounterStrategy)
    :code.purge(CounterStrategy)
  end

  test "an error is raised if retry/3 returns an invalid value", %{conn: conn} do
    defmodule InvalidStrategy do
      @behaviour Xandra.RetryStrategy

      def new([{_pid, _host}], _options), do: %{}
      def retry(_error, _options, _state), do: :invalid_value
    end

    message =
      "invalid return value :invalid_value from " <>
        "retry strategy Xandra.RetryStrategiesTest.InvalidStrategy with state %{}"

    assert_raise ArgumentError, message, fn ->
      Xandra.Cluster.execute(conn, "USE nonexistend_keyspace", [],
        retry_strategy: InvalidStrategy
      )
    end
  after
    :code.delete(InvalidStrategy)
    :code.purge(InvalidStrategy)
  end
end
