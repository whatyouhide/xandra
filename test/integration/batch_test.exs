defmodule BatchTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.{Batch, Error, Void, Page}

  setup_all %{keyspace: keyspace, setup_conn: setup_conn} do
    Xandra.execute!(setup_conn, "USE #{keyspace}")
    Xandra.execute!(setup_conn, "CREATE TABLE users (id int, name text, PRIMARY KEY (id))")
    :ok
  end

  setup %{setup_conn: conn} do
    Xandra.execute!(conn, "TRUNCATE users")
    :ok
  end

  test "batch of type \"logged\"", %{keyspace: keyspace, conn: conn} do
    statement = "INSERT INTO #{keyspace}.users (id, name) VALUES (:id, :name)"
    prepared_insert = Xandra.prepare!(conn, statement)

    batch =
      Batch.new(:logged)
      |> Batch.add("INSERT INTO users (id, name) VALUES (1, 'Marge')")
      |> Batch.add(prepared_insert, [2, "Homer"])
      |> Batch.add("INSERT INTO users (id, name) VALUES (?, ?)", [{"int", 3}, {"text", "Lisa"}])
      |> Batch.add("DELETE FROM users WHERE id = ?", [{"int", 3}])

    assert {:ok, %Void{}} = Xandra.execute(conn, batch)

    {:ok, result} = Xandra.execute(conn, "SELECT name FROM users")

    assert Enum.to_list(result) == [
             %{"name" => "Marge"},
             %{"name" => "Homer"}
           ]
  end

  test "batch of type \"unlogged\"", %{conn: conn} do
    batch =
      Batch.new(:unlogged)
      |> Batch.add("INSERT INTO users (id, name) VALUES (1, 'Rick')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (2, 'Morty')")

    assert {:ok, %Void{}} = Xandra.execute(conn, batch)

    result = Xandra.execute!(conn, "SELECT name FROM users")

    assert Enum.to_list(result) == [
             %{"name" => "Rick"},
             %{"name" => "Morty"}
           ]
  end

  test "using a default timestamp for the batch", %{conn: conn} do
    timestamp = System.system_time(:second) - (_10_minutes = 600)

    batch =
      Batch.new()
      |> Batch.add("INSERT INTO users (id, name) VALUES (1, 'Abed')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (2, 'Troy')")

    assert {:ok, %Void{}} = Xandra.execute(conn, batch, timestamp: timestamp)

    result = Xandra.execute!(conn, "SELECT name, WRITETIME(name) FROM users")

    assert Enum.to_list(result) == [
             %{"name" => "Abed", "writetime(name)" => timestamp},
             %{"name" => "Troy", "writetime(name)" => timestamp}
           ]
  end

  test "errors when there are bad queries in the batch", %{conn: conn} do
    # Only INSERT, UPDATE, and DELETE statements are allowed in BATCH queries.
    invalid_batch = Batch.add(Batch.new(), "SELECT * FROM users")
    assert {:error, %Error{reason: :invalid}} = Xandra.execute(conn, invalid_batch)
  end

  test "named params are supported in prepared queries even if they aren't in the protocol", %{
    conn: conn,
    keyspace: keyspace
  } do
    statement = "INSERT INTO #{keyspace}.users (id, name) VALUES (:id, :name)"
    prepared_insert = Xandra.prepare!(conn, statement)

    batch = Batch.add(Batch.new(), prepared_insert, %{"id" => 1, "name" => "Beth"})

    assert {:ok, %Void{}} = Xandra.execute(conn, batch)

    result = Xandra.execute!(conn, "SELECT name FROM users WHERE id = 1")

    assert Enum.to_list(result) == [
             %{"name" => "Beth"}
           ]
  end

  @tag start_conn: false
  test "an error is raised if named parameters are used with simple queries" do
    message = ~r/non-prepared statements inside batch queries only support positional/

    assert_raise ArgumentError, message, fn ->
      statement = "INSERT INTO users (id, name) VALUES (:id, :name)"
      Batch.add(Batch.new(), statement, %{"id" => 1, "name" => "Summer"})
    end
  end

  test "an error is raised if a named parameter is missing for prepared queries", %{
    conn: conn,
    keyspace: keyspace
  } do
    message = "missing named parameter \"name\" for prepared query, got: %{\"id\" => 1}"

    assert_raise ArgumentError, message, fn ->
      statement = "INSERT INTO #{keyspace}.users (id, name) VALUES (:id, :name)"
      prepared_insert = Xandra.prepare!(conn, statement)
      batch = Batch.add(Batch.new(), prepared_insert, %{"id" => 1})
      Xandra.execute(conn, batch)
    end
  end

  test "empty batch", %{conn: conn} do
    assert {:ok, %Void{}} = Xandra.execute(conn, Batch.new())
  end

  test "inspecting batch queries", %{conn: conn, keyspace: keyspace} do
    prepared = Xandra.prepare!(conn, "DELETE FROM #{keyspace}.users WHERE id = ?")

    batch =
      Batch.new(:logged)
      |> Batch.add("INSERT INTO users (id, name) VALUES (1, 'Marge')")
      |> Batch.add(prepared, [2])

    expected =
      ~s/#Xandra.Batch<[type: :logged, / <>
        ~s/queries: [{"INSERT INTO users (id, name) VALUES (1, 'Marge')", []}, / <>
        ~s/{#Xandra.Prepared<[statement: "DELETE FROM #{keyspace}.users WHERE id = ?", tracing_id: nil]>, [2]}]]>/

    assert inspect(batch) == expected
  end

  @tag :cassandra_specific
  test "batch query with lightweight transactions", %{conn: conn} do
    batch =
      Batch.new(:logged)
      |> Batch.add("INSERT INTO users (id, name) VALUES (1, 'Marge')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (1, 'Bart') IF NOT EXISTS")

    assert {:ok, %Page{}} = Xandra.execute(conn, batch)
  end

  # Regression for a bug we had (at Veeps) where we saw queries being reprepared, and
  # Xandra would pass the execute/3 options down to prepare/3, which fails on validating
  # most of those options.
  test "if given prepared queries, potentially reprepares them", %{conn: conn, keyspace: keyspace} do
    statement = "INSERT INTO #{keyspace}.users (id, name) VALUES (:id, :name)"
    prepared_insert = Xandra.prepare!(conn, statement)

    # Hacky, but works.
    prepared_insert = %Xandra.Prepared{prepared_insert | id: "invalid", result_columns: nil}

    batch =
      Batch.new(:logged)
      |> Batch.add(prepared_insert, [1, "Homer"])
      |> Batch.add(prepared_insert, [2, "Marge"])

    assert {:ok, %Void{}} = Xandra.execute(conn, batch)

    {:ok, result} = Xandra.execute(conn, "SELECT name FROM users")

    assert Enum.to_list(Enum.sort(result)) == [
             %{"name" => "Homer"},
             %{"name" => "Marge"}
           ]
  end
end
