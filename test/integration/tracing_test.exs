defmodule TracingTest do
  use XandraTest.IntegrationCase

  alias Xandra.Batch

  @moduletag :cosmosdb_unsupported

  setup_all %{keyspace: keyspace, start_options: start_options} do
    {:ok, conn} = Xandra.start_link(start_options)
    Xandra.execute!(conn, "USE #{keyspace}")

    statement = "CREATE TABLE users (id int, name text, PRIMARY KEY (id))"
    Xandra.execute!(conn, statement)

    :ok
  end

  setup %{conn: conn} do
    Xandra.execute!(conn, "TRUNCATE users")
    :ok
  end

  describe "tracing enabled in each possible response" do
    test "Xandra.Page", %{conn: conn} do
      page = Xandra.execute!(conn, "SELECT * FROM users", [], tracing: true)
      assert is_binary(page.tracing_id)
    end

    test "Xandra.SetKeyspace", %{conn: conn} do
      set_keyspace = Xandra.execute!(conn, "USE system", [], tracing: true)
      assert is_binary(set_keyspace.tracing_id)
    end

    test "Xandra.SchemaChange", %{conn: conn} do
      statement = "CREATE TABLE schema_change_with_tracing (id int, name text, PRIMARY KEY (id))"
      schema_change = Xandra.execute!(conn, statement, [], tracing: true)
      assert is_binary(schema_change.tracing_id)
    end

    test "Xandra.Void", %{conn: conn} do
      void = Xandra.execute!(conn, "TRUNCATE users", [], tracing: true)
      assert is_binary(void.tracing_id)
    end

    test "Xandra.Prepared", %{conn: conn} do
      prepared = Xandra.prepare!(conn, "USE system", tracing: true)
      assert is_binary(prepared.tracing_id)
    end
  end

  test "tracing enabled when executing simple, prepared, and batch queryes",
       %{conn: conn, keyspace: keyspace} do
    result = Xandra.execute!(conn, "USE system", [], tracing: true)
    assert is_binary(result.tracing_id)

    prepared = Xandra.prepare!(conn, "USE system")
    result = Xandra.execute!(conn, prepared, [], tracing: true)
    assert is_binary(result.tracing_id)

    batch = Batch.new(:logged) |> Batch.add("DELETE FROM #{keyspace}.users WHERE id = 1")
    result = Xandra.execute!(conn, batch, tracing: true)
    assert is_binary(result.tracing_id)
  end

  test "tracing a query and reading its trace", %{conn: conn} do
    result = Xandra.execute!(conn, "USE system", [], tracing: true)

    statement = "SELECT * FROM system_traces.events WHERE session_id = ?"

    assert {:ok, _trace_events_page} =
             Xandra.execute(conn, statement, [{"uuid", result.tracing_id}])
  end
end
