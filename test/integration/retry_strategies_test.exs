defmodule Xandra.RetryStrategiesTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.Error

  test "strategy that retries for a fixed amount of times", %{conn: conn} do
    defmodule CounterStrategy do
      @behaviour Xandra.RetryStrategy

      def new(options) do
        Keyword.fetch!(options, :retries_count)
      end

      def retry(_error, _options, 0) do
        :error
      end

      def retry(error, options, retries_count) do
        send(self(), {:retrying, error, retries_count})
        {:retry, options, retries_count - 1}
      end
    end

    assert_raise KeyError, fn ->
      Xandra.execute(conn, "USE nonexistent_keyspace", [], retry_strategy: CounterStrategy)
    end

    options = [retry_strategy: CounterStrategy, retries_count: 2]
    assert {:error, _} = Xandra.execute(conn, "USE nonexistent_keyspace", [], options)

    assert_received {:retrying, %Error{reason: :invalid}, 2}
    assert_received {:retrying, %Error{reason: :invalid}, 1}
  after
    :code.delete(CounterStrategy)
    :code.purge(CounterStrategy)
  end

  test "an error is raised if retry/3 returns an invalid value", %{conn: conn} do
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
