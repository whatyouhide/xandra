defmodule PreparedTest do
  use XandraTest.IntegrationCase, async: true

  setup_all %{keyspace: keyspace, start_options: start_options} do
    {:ok, conn} = Xandra.start_link(start_options)
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

  @tag protocol_version: :v4
  test "unset values in prepared statements", %{conn: conn} do
    statement = "CREATE TABLE cars (id int PRIMARY KEY, name text)"
    Xandra.execute!(conn, statement)

    statement = "INSERT INTO cars (id, name) VALUES (?, ?)"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)
    assert {:ok, %Xandra.Void{}} = Xandra.execute(conn, prepared, [1, "Tesla"])
    assert {:ok, %Xandra.Void{}} = Xandra.execute(conn, prepared, [2, "BMW"])
    assert {:ok, %Xandra.Void{}} = Xandra.execute(conn, prepared, [3, "T3"])

    assert {:ok, page} = Xandra.execute(conn, "SELECT id, name FROM cars")

    assert Enum.to_list(page) == [
             %{"id" => 1, "name" => "Tesla"},
             %{"id" => 2, "name" => "BMW"},
             %{"id" => 3, "name" => "T3"}
           ]

    assert {:ok, %Xandra.Void{}} = Xandra.execute(conn, prepared, [1, "Mercedes"])
    assert {:ok, %Xandra.Void{}} = Xandra.execute(conn, prepared, [2, nil])
    assert {:ok, %Xandra.Void{}} = Xandra.execute(conn, prepared, [3, :unset])

    assert {:ok, page} = Xandra.execute(conn, "SELECT id, name FROM cars")

    assert Enum.to_list(page) == [
             %{"id" => 1, "name" => "Mercedes"},
             %{"id" => 2, "name" => nil},
             %{"id" => 3, "name" => "T3"}
           ]
  end

  @tag protocol_version: :v3
  test "unset value in v3 crashes", %{conn: conn} do
    statement = "INSERT INTO users (code, name) VALUES (?, ?)"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)

    assert_raise FunctionClauseError,
                 "no function clause matching in Xandra.Protocol.V3.encode_value/2",
                 fn ->
                   Xandra.execute!(conn, prepared, [3, :unset])
                 end
  end

  @tag protocol_version: :v4
  test "unset values in prepared statements 2", %{conn: conn} do
    Xandra.execute!(conn, "DROP TABLE IF EXISTS towns")
    statement = "CREATE TABLE towns (id int PRIMARY KEY, name text, location text)"
    Xandra.execute!(conn, statement)

    statement = "INSERT INTO towns (id, name, location) VALUES (?, ?, ?)"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)

    for i <- 1..100 do
      assert {:ok, %Xandra.Void{}} = Xandra.execute(conn, prepared, [i, "town#{i}", :unset])
    end
  end

  @tag protocol_version: :v4
  test "unset values in simple statements", %{conn: conn} do
    Xandra.execute!(conn, "DROP TABLE IF EXISTS towns")
    statement = "CREATE TABLE towns (id int PRIMARY KEY, name text, location text)"
    Xandra.execute!(conn, statement)

    statement = "INSERT INTO towns (id, name, location) VALUES (?, ?, ?)"

    for i <- 1..100 do
      assert {:ok, %Xandra.Void{}} =
               Xandra.execute(conn, statement, [
                 {"int", i},
                 {"text", "simple_town#{i}"},
                 {"text", :unset}
               ])
    end
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
