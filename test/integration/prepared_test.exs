defmodule PreparedTest do
  use XandraTest.IntegrationCase, async: true

  setup_all %{keyspace: keyspace, setup_conn: setup_conn} do
    Xandra.execute!(setup_conn, "USE #{keyspace}")

    statement = "CREATE TABLE users (code int, name text, PRIMARY KEY (code, name))"
    Xandra.execute!(setup_conn, statement)

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

    Xandra.execute!(setup_conn, statement)

    :ok
  end

  test "prepared functionality", %{keyspace: keyspace, conn: conn} do
    statement = "SELECT name FROM #{keyspace}.users WHERE code = :code"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)
    # Successive call to prepare uses cache.
    assert {:ok, ^prepared} = Xandra.prepare(conn, statement)

    assert {:ok, page} = Xandra.execute(conn, prepared, [1])

    assert Enum.to_list(page) == [
             %{"name" => "Homer"},
             %{"name" => "Lisa"},
             %{"name" => "Marge"}
           ]

    assert {:ok, page} = Xandra.execute(conn, prepared, %{"code" => 2})

    assert Enum.to_list(page) == [
             %{"name" => "Moe"}
           ]

    assert {:ok, page} = Xandra.execute(conn, prepared, %{"code" => 5})
    assert Enum.to_list(page) == []
  end

  @tag :cassandra_specific
  test "dynamic result columns", %{keyspace: keyspace, conn: conn} do
    statement = "INSERT INTO #{keyspace}.users (code, name) VALUES (3, 'Nelson') IF NOT EXISTS"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)

    assert {:ok, page} = Xandra.execute(conn, prepared)
    assert Enum.to_list(page) == [%{"[applied]" => true}]

    assert {:ok, page} = Xandra.execute(conn, prepared)
    assert Enum.to_list(page) == [%{"[applied]" => false, "code" => 3, "name" => "Nelson"}]
  end

  test "inspecting prepared queries", %{keyspace: keyspace, conn: conn} do
    prepared = Xandra.prepare!(conn, "SELECT * FROM #{keyspace}.users")

    assert inspect(prepared) ==
             ~s(#Xandra.Prepared<[statement: "SELECT * FROM #{keyspace}.users", tracing_id: nil]>)
  end

  test "missing named params raise an error", %{keyspace: keyspace, conn: conn} do
    message = "missing named parameter \"name\" for prepared query, got: %{\"code\" => 1}"

    assert_raise ArgumentError, message, fn ->
      statement = "INSERT INTO #{keyspace}.users (code, name) VALUES (:code, :name)"
      prepared_insert = Xandra.prepare!(conn, statement)
      Xandra.execute(conn, prepared_insert, %{"code" => 1})
    end
  end
end
