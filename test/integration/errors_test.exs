defmodule ErrorsTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.Error

  test "each possible error", %{conn: conn} do
    assert {:error, reason} = Xandra.execute(conn, "", [])
    assert %Error{reason: :invalid_syntax} = reason

    assert {:error, reason} = Xandra.execute(conn, "USE unknown", [])
    assert %Error{reason: :invalid} = reason

    {:ok, _void} = Xandra.execute(conn, "CREATE TABLE errors (id int PRIMARY KEY)", [])
    assert {:error, reason} = Xandra.execute(conn, "CREATE TABLE errors (id int PRIMARY KEY)", [])
    assert %Error{reason: :already_exists} = reason

    {:ok, query} = Xandra.prepare(conn, "SELECT * FROM errors", [])
    assert {:error, reason} = Xandra.execute(conn, %{query | id: <<>>}, [])
    assert %Error{reason: :unprepared} = reason
  end
end
