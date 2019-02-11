defmodule ResultsTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.{SchemaChange, SetKeyspace, Void, Simple}

  test "each possible result", %{conn: conn, keyspace: keyspace} do
    assert {:ok, %Simple{}, result} = Xandra.execute(conn, "USE #{keyspace}")
    assert result == %SetKeyspace{keyspace: String.downcase(keyspace)}

    statement = "CREATE TABLE numbers (figure int PRIMARY KEY)"
    assert {:ok, %Simple{}, result} = Xandra.execute(conn, statement)

    assert result == %SchemaChange{
             effect: "CREATED",
             options: %{
               keyspace: String.downcase(keyspace),
               subject: "numbers"
             },
             target: "TABLE"
           }

    statement = "INSERT INTO numbers (figure) VALUES (123)"
    assert {:ok, %Simple{}, result} = Xandra.execute(conn, statement)
    assert result == %Void{}

    statement = "SELECT * FROM numbers WHERE figure = ?"
    assert {:ok, %Simple{}, result} = Xandra.execute(conn, statement, [{"int", 123}])
    assert Enum.to_list(result) == [%{"figure" => 123}]

    assert {:ok, %Simple{}, result} = Xandra.execute(conn, statement, [{"int", 321}])
    assert Enum.to_list(result) == []

    statement = "SELECT * FROM numbers WHERE figure = :figure"
    assert {:ok, %Simple{}, result} = Xandra.execute(conn, statement, %{"figure" => {"int", 123}})
    assert Enum.to_list(result) == [%{"figure" => 123}]
  end

  test "inspecting Xandra.Page results", %{conn: conn} do
    Xandra.execute!(conn, "CREATE TABLE users (name text PRIMARY KEY)")
    Xandra.execute!(conn, "INSERT INTO users (name) VALUES ('Jeff')")
    %Xandra.Page{} = page = Xandra.execute!(conn, "SELECT * FROM users")
    assert inspect(page) == ~s(#Xandra.Page<[rows: [%{"name" => "Jeff"}], more_pages?: false]>)
  end
end
