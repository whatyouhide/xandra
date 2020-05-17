defmodule PreparedTest do
  use XandraTest.IntegrationCase, async: true

  setup_all %{keyspace: keyspace, start_options: start_options} do
    {:ok, conn} = Xandra.start_link(start_options)
    Xandra.execute!(conn, "USE #{keyspace}")

    statement = "CREATE TABLE users (code int, name text, PRIMARY KEY (code, name))"
    Xandra.execute!(conn, statement)

    statement = """
    BEGIN UNLOGGED BATCH
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
    # Successive call to prepare uses cache. This removes the custom payload (if any).
    prepared = %{prepared | custom_payload: nil}
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
  test "dynamic result columns", %{conn: conn} do
    statement = "INSERT INTO users (code, name) VALUES (3, 'Nelson') IF NOT EXISTS"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)

    assert {:ok, page} = Xandra.execute(conn, prepared)
    assert Enum.to_list(page) == [%{"[applied]" => true}]

    assert {:ok, page} = Xandra.execute(conn, prepared)
    assert Enum.to_list(page) == [%{"[applied]" => false, "code" => 3, "name" => "Nelson"}]
  end

  test "inspecting prepared queries", %{conn: conn, is_cosmosdb: is_cosmosdb} do
    prepared = Xandra.prepare!(conn, "SELECT * FROM users")

    expected =
      ~s(#Xandra.Prepared<[statement: "SELECT * FROM users", tracing_id: nil, custom_payload: ) <>
        if is_cosmosdb do
          ~s([{"RequestCharge", <<63, 249, 29, 89, 218, 64, 181, 66>>}])
        else
          "nil"
        end <>
        ~s(]>)

    assert inspect(prepared) == expected
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
