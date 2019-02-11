defmodule PreparedTest do
  use XandraTest.IntegrationCase, async: true

  setup_all %{keyspace: keyspace} do
    {:ok, conn} = Xandra.start_link()
    Xandra.execute!(conn, "USE #{keyspace}")

    statement = "CREATE TABLE users (code int, name text, PRIMARY KEY (code, name))"
    Xandra.execute!(conn, statement)

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

    Xandra.execute!(conn, statement)

    :ok
  end

  test "prepared functionality", %{conn: conn} do
    statement = "SELECT name FROM users WHERE code = :code"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)
    # Successive call to prepare uses cache.
    assert {:ok, ^prepared} = Xandra.prepare(conn, statement)

    assert {:ok, ^prepared, page} = Xandra.execute(conn, prepared, [1])

    assert Enum.to_list(page) == [
             %{"name" => "Homer"},
             %{"name" => "Lisa"},
             %{"name" => "Marge"}
           ]

    assert {:ok, ^prepared, page} = Xandra.execute(conn, prepared, %{"code" => 2})

    assert Enum.to_list(page) == [
             %{"name" => "Moe"}
           ]

    assert {:ok, ^prepared, page} = Xandra.execute(conn, prepared, %{"code" => 5})
    assert Enum.to_list(page) == []
  end

  test "dynamic result columns", %{conn: conn} do
    statement = "INSERT INTO users (code, name) VALUES (3, 'Nelson') IF NOT EXISTS"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)

    assert {:ok, ^prepared, page} = Xandra.execute(conn, prepared)
    assert Enum.to_list(page) == [%{"[applied]" => true}]

    assert {:ok, ^prepared, page} = Xandra.execute(conn, prepared)
    assert Enum.to_list(page) == [%{"[applied]" => false, "code" => 3, "name" => "Nelson"}]
  end

  test "inspecting prepared queries", %{conn: conn} do
    prepared = Xandra.prepare!(conn, "SELECT * FROM users")
    assert inspect(prepared) == ~s(#Xandra.Prepared<"SELECT * FROM users">)
  end

  test "missing named params raise an error", %{conn: conn} do
    message = "missing named parameter \"name\" for prepared query, got: %{\"code\" => 1}"

    assert_raise ArgumentError, message, fn ->
      statement = "INSERT INTO users (code, name) VALUES (:code, :name)"
      prepared_insert = Xandra.prepare!(conn, statement)
      Xandra.execute(conn, prepared_insert, %{"code" => 1})
    end
  end
end
