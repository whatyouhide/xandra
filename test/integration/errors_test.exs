defmodule ErrorsTest do
  use XandraTest.IntegrationCase

  alias Xandra.Error

  test "each possible error", %{conn: conn, is_cosmosdb: is_cosmosdb} do
    assert {:error, reason} = Xandra.execute(conn, "")
    assert %Error{reason: :invalid_syntax} = reason

    assert {:error, reason} = Xandra.execute(conn, "USE unknown")
    assert %Error{reason: :invalid} = reason

    Xandra.execute!(conn, "CREATE TABLE errors (id int PRIMARY KEY, reason text)")
    assert {:error, reason} = Xandra.execute(conn, "CREATE TABLE errors (id int PRIMARY KEY)")
    assert %Error{reason: :already_exists} = reason

    assert {:error, reason} = Xandra.prepare(conn, "SELECT * FROM unknown")

    if is_cosmosdb do
      assert %Error{reason: :invalid_config} = reason
    else
      assert %Error{reason: :invalid} = reason
    end
  end

  @tag :cassandra_specific
  test "function_failure error", %{keyspace: keyspace, start_options: start_options} do
    # This is only supported in native protocol v4.
    start_options = Keyword.put(start_options, :protocol_version, :v4)

    {:ok, conn} = Xandra.start_link(start_options)
    Xandra.execute!(conn, "USE #{keyspace}")
    Xandra.execute!(conn, "CREATE TABLE funs (id int PRIMARY KEY, name text)")

    Xandra.execute!(conn, """
    CREATE FUNCTION thrower (x int) CALLED ON NULL INPUT
    RETURNS int LANGUAGE java AS 'throw new RuntimeException();'
    """)

    Xandra.execute!(conn, "INSERT INTO funs (id, name) VALUES (1, 'my_fun')")

    assert {:error, %Error{} = error} = Xandra.execute(conn, "SELECT thrower(id) FROM funs")
    assert error.reason == :function_failure
    assert error.message =~ "java.lang.RuntimeException"
  end

  test "errors are raised from bang! functions", %{conn: conn} do
    assert_raise Error, fn -> Xandra.prepare!(conn, "") end
    assert_raise Error, fn -> Xandra.execute!(conn, "USE unknown") end
  end
end
