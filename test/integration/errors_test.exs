defmodule ErrorsTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.Error

  test "each possible error", %{conn: conn} do
    assert {:error, reason} = Xandra.execute(conn, "")
    assert %Error{reason: :invalid_syntax} = reason

    assert {:error, reason} = Xandra.execute(conn, "USE unknown")
    assert %Error{reason: :invalid} = reason

    Xandra.execute!(conn, "CREATE TABLE errors (id int PRIMARY KEY, reason text)")
    assert {:error, reason} = Xandra.execute(conn, "CREATE TABLE errors (id int PRIMARY KEY)")
    assert %Error{reason: :already_exists} = reason

    assert {:error, reason} = Xandra.prepare(conn, "SELECT * FROM unknown")
    assert %Error{reason: :invalid} = reason
  end

  test "errors are raised from bang! functions", %{conn: conn} do
    assert_raise Error, fn -> Xandra.prepare!(conn, "") end
    assert_raise Error, fn -> Xandra.execute!(conn, "USE unknown") end
  end
end
