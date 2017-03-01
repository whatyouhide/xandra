defmodule Xandra.RetryStrategiesTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.Error

  test "ignoring errors", %{conn: conn} do
    defmodule IgnoreStrategy do
      @behaviour Xandra.RetryStrategy

      def init(_opts), do: :ok

      def handle_retry(error, _options, :ok) do
        send(self(), {:retrying, error})
        :ignore
      end
    end

    assert Xandra.execute!(conn, "USE nonexistent_keyspace", [], retry_strategy: IgnoreStrategy) ==
           :failed

    assert_received {:retrying, %Error{reason: :invalid}}
  after
    :code.delete(IgnoreStrategy)
    :code.purge(IgnoreStrategy)
  end

  test "strategy that retries for a fixed amount of times", %{conn: conn} do
    defmodule CounterStrategy do
      @behaviour Xandra.RetryStrategy

      def init(options) do
        Keyword.fetch!(options, :retries_count)
      end

      def handle_retry(_error, _options, 0) do
        :error
      end

      def handle_retry(error, options, retries_count) do
        send(self(), {:retrying, error, retries_count})
        {:retry, options, retries_count - 1}
      end
    end

    assert_raise KeyError, fn ->
      Xandra.execute(conn, "USE nonexistend_keyspace", [], retry_strategy: CounterStrategy)
    end

    options = [retry_strategy: CounterStrategy, retries_count: 2]
    assert {:error, _} = Xandra.execute(conn, "USE nonexistend_keyspace", [], options)

    assert_received {:retrying, %Error{reason: :invalid}, 2}
    assert_received {:retrying, %Error{reason: :invalid}, 1}
  after
    :code.delete(CounterStrategy)
    :code.purge(CounterStrategy)
  end

  test "an error is raised if an handle_retry/3 returns an invalid value", %{conn: conn} do
    defmodule BadStrategy do
      @behaviour Xandra.RetryStrategy

      def init(_options), do: %{}
      def handle_retry(_error, _options, _state), do: :bad_return_value
    end

    message =
      "invalid return value from retry strategy Xandra.RetryStrategiesTest.BadStrategy " <>
      "with strategy %{}: :bad_return_value"
    assert_raise ArgumentError, message, fn ->
      Xandra.execute(conn, "USE nonexistend_keyspace", [], retry_strategy: BadStrategy)
    end
  after
    :code.delete(BadStrategy)
    :code.purge(BadStrategy)
  end
end
