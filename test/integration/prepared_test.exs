defmodule PreparedTest do
  use XandraTest.IntegrationCase, async: true

  setup_all %{keyspace: keyspace} do
    {:ok, conn} = Xandra.start_link()
    {:ok, _void} = Xandra.execute(conn, "USE #{keyspace}", [])

    statement = "CREATE TABLE users (code int, name text, PRIMARY KEY (code, name))"
    {:ok, _result} = Xandra.execute(conn, statement, [])

    statement = """
    BEGIN BATCH
    INSERT INTO users (code, name) VALUES (1, 'Marge');
    INSERT INTO users (code, name) VALUES (1, 'Homer');
    INSERT INTO users (code, name) VALUES (1, 'Lisa');
    INSERT INTO users (code, name) VALUES (2, 'Moe');
    INSERT INTO users (code, name) VALUES (3, 'Ned');
    INSERT INTO users (code, name) VALUES (3, 'Burns');
    INSERT INTO users (code, name) VALUES (4, 'Bob');
    APPLY BATCH
    """
    {:ok, _result} = Xandra.execute(conn, statement, [])

    :ok
  end

  test "prepared functionality", %{conn: conn} do
    statement = "SELECT name FROM users WHERE code = :code"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)

    assert {:ok, rows} = Xandra.execute(conn, prepared, [1])
    assert Enum.to_list(rows) == [
      %{"name" => "Homer"}, %{"name" => "Lisa"}, %{"name" => "Marge"}
    ]

    assert {:ok, rows} = Xandra.execute(conn, prepared, %{"code" => 2})
    assert Enum.to_list(rows) == [
      %{"name" => "Moe"}
    ]

    assert {:ok, rows} = Xandra.execute(conn, prepared, %{"code" => 5})
    assert Enum.to_list(rows) == []

    statement = "SELECT name FROM users WHERE code = ?"
    assert {:ok, prepared, rows} = Xandra.prepare_execute(conn, statement, [3])
    assert Enum.to_list(rows) == [
      %{"name" => "Burns"}, %{"name" => "Ned"}
    ]

    statement = "SELECT name FROM users WHERE code = ?"
    assert {:ok, ^prepared, rows} = Xandra.prepare_execute(conn, statement, [4])
    assert Enum.to_list(rows) == [
      %{"name" => "Bob"}
    ]
  end
end
