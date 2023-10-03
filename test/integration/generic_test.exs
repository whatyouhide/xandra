defmodule GenericTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.ConnectionError

  # Regression for https://github.com/lexhide/xandra/issues/266
  # This test doesn't test much for now, it's sort of a smoke test. Once we'll have
  # generic telemetry events for queries, we can change this test to assert on the telemetry
  # event.
  test ":default_consistency option when starting", %{
    keyspace: keyspace,
    start_options: start_options
  } do
    assert {:ok, test_conn} =
             start_supervised({Xandra, [default_consistency: :three] ++ start_options})

    Xandra.execute!(test_conn, "USE #{keyspace}")
  end

  test ":keyspace option when starting", %{keyspace: keyspace, start_options: start_options} do
    assert {:ok, test_conn} = start_supervised({Xandra, [keyspace: keyspace] ++ start_options})

    assert {:connected, state} = :sys.get_state(test_conn)
    assert state.current_keyspace == keyspace
  end

  test ":timeout option with Xandra.execute/3", %{conn: conn} do
    assert {:error, %ConnectionError{} = error} =
             Xandra.execute(conn, "SELECT * FROM system.local", [], timeout: 0)

    assert error.reason == :timeout
  end

  test ":timeout option with Xandra.prepare/3", %{conn: conn} do
    assert {:error, %ConnectionError{} = error} =
             Xandra.prepare(conn, "SELECT * FROM system.local", timeout: 0)

    assert error.reason == :timeout
  end

  describe ":configure start option" do
    test "as an anonymous function", %{start_options: start_options} do
      test_pid = self()
      ref = make_ref()

      configure_fun = fn options ->
        send(test_pid, {ref, options})
        Keyword.replace!(options, :nodes, start_options[:nodes])
      end

      modified_start_options =
        Keyword.merge(start_options, configure: configure_fun, nodes: ["localhost:9999"])

      assert {:ok, _test_conn} = start_supervised({Xandra, modified_start_options})

      assert_receive {^ref, configure_start_options}
      assert configure_start_options[:node] == {~c"localhost", 9999}
    end

    test "as MFA", %{start_options: start_options} do
      ref = make_ref()

      configure_fun = {__MODULE__, :configure_fun, [start_options, self(), ref]}

      modified_start_options =
        Keyword.merge(start_options, configure: configure_fun, nodes: ["localhost:9999"])

      assert {:ok, _test_conn} = start_supervised({Xandra, modified_start_options})

      assert_receive {^ref, configure_start_options}
      assert configure_start_options[:node] == {~c"localhost", 9999}
    end
  end

  def configure_fun(options, original_start_options, pid, ref) do
    send(pid, {ref, options})
    Keyword.replace!(options, :nodes, original_start_options[:nodes])
  end
end
