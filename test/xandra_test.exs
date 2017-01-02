defmodule XandraTest do
  use ExUnit.Case

  alias Xandra.{SchemaChange, Void}

  defp with_test_keyspace(fun) do
    {:ok, conn} = Xandra.start_link()
    try do
      {:ok, _} = Xandra.execute(conn, "CREATE KEYSPACE xandra_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", [], [])
      {:ok, _} = Xandra.execute(conn, "USE xandra_test", [], [])
      fun.(conn)
    after
      Xandra.execute(conn, "DROP KEYSPACE xandra_test", [], [])
    end
  end

  test "basic interactions" do
    with_test_keyspace(fn conn ->
      statement = "CREATE TABLE users (code int, name text, PRIMARY KEY (code, name))"
      {:ok, result} = Xandra.execute(conn, statement, [])
      assert result == %SchemaChange{
        effect: "CREATED",
        options: %{
          keyspace: "xandra_test",
          subject: "users",
        },
        target: "TABLE",
      }

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
      {:ok, result} = Xandra.execute(conn, statement, [])
      assert result == %Void{}

      statement = "SELECT name FROM users WHERE code = :code"
      {:ok, result} = Xandra.execute(conn, statement, %{"code" => {"int", 3}})
      assert Enum.to_list(result) == [
        %{"name" => "Burns"}, %{"name" => "Ned"}
      ]

      statement = "SELECT name FROM users WHERE code = :code"
      {:ok, query} = Xandra.prepare(conn, statement)

      {:ok, result} = Xandra.execute(conn, query, %{"code" => {"int", 1}})
      assert Enum.to_list(result) == [
        %{"name" => "Homer"}, %{"name" => "Lisa"}, %{"name" => "Marge"}
      ]
      {:ok, result} = Xandra.execute(conn, query, %{"code" => {"int", 2}})
      assert Enum.to_list(result) == [
        %{"name" => "Moe"}
      ]
    end)
  end
end
