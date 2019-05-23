defmodule BatchTest do
  use XandraTest.IntegrationCase

  alias Xandra.{Batch, Error, Void}

  setup_all %{keyspace: keyspace, start_options: start_options} do
    {:ok, conn} = Xandra.start_link(start_options)
    Xandra.execute!(conn, "USE #{keyspace}")

    statement = "CREATE TABLE users (id int, name text, PRIMARY KEY (id))"
    Xandra.execute!(conn, statement)

    :ok
  end

  setup %{conn: conn} do
    Xandra.execute!(conn, "TRUNCATE users")
    :ok
  end

  test "batch of type \"logged\"", %{conn: conn} do
    statement = "INSERT INTO users (id, name) VALUES (:id, :name)"
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

  @tag protocol_version: :v4
  test "batch of type \"unlogged\" producing warning in v4", %{conn: conn} do
    # batches spanning more partitions than `unlogged_batch_across_partitions_warn_threshold`
    #   (default: 10) generate a warning
    batch =
      Batch.new(:unlogged)
      |> Batch.add("INSERT INTO users (id, name) VALUES (1, 'Rick')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (2, 'Morty')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (3, 'Beth')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (4, 'Jerry')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (5, 'Summer')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (6, 'Jessica')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (7, 'Gene')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (8, 'Feratu')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (9, 'Brad')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (10, 'Ethan')")
      |> Batch.add("INSERT INTO users (id, name) VALUES (11, 'Nancy')")

    {:ok, %Void{}} = Xandra.execute(conn, batch)

    result =
      Xandra.execute!(conn, "SELECT name FROM users")
      |> Enum.flat_map(&Map.values/1)
      |> Enum.sort()

    assert result == [
             "Beth",
             "Brad",
             "Ethan",
             "Feratu",
             "Gene",
             "Jerry",
             "Jessica",
             "Morty",
             "Nancy",
             "Rick",
             "Summer"
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
    conn: conn
  } do
    statement = "INSERT INTO users (id, name) VALUES (:id, :name)"
    prepared_insert = Xandra.prepare!(conn, statement)

    batch = Batch.add(Batch.new(), prepared_insert, %{"id" => 1, "name" => "Beth"})

    assert {:ok, %Void{}} = Xandra.execute(conn, batch)

    result = Xandra.execute!(conn, "SELECT name FROM users WHERE id = 1")

    assert Enum.to_list(result) == [
             %{"name" => "Beth"}
           ]
  end

  test "an error is raised if named parameters are used with simple queries" do
    message = ~r/non-prepared statements inside batch queries only support positional/

    assert_raise ArgumentError, message, fn ->
      statement = "INSERT INTO users (id, name) VALUES (:id, :name)"
      Batch.add(Batch.new(), statement, %{"id" => 1, "name" => "Summer"})
    end
  end

  test "an error is raised if a named parameter is missing for prepared queries", %{conn: conn} do
    message = "missing named parameter \"name\" for prepared query, got: %{\"id\" => 1}"

    assert_raise ArgumentError, message, fn ->
      statement = "INSERT INTO users (id, name) VALUES (:id, :name)"
      prepared_insert = Xandra.prepare!(conn, statement)
      batch = Batch.add(Batch.new(), prepared_insert, %{"id" => 1})
      Xandra.execute(conn, batch)
    end
  end

  test "empty batch", %{conn: conn} do
    assert {:ok, %Void{}} = Xandra.execute(conn, Batch.new())
  end

  test "inspecting batch queries", %{conn: conn} do
    prepared = Xandra.prepare!(conn, "DELETE FROM users WHERE id = ?")

    batch =
      Batch.new(:logged)
      |> Batch.add("INSERT INTO users (id, name) VALUES (1, 'Marge')")
      |> Batch.add(prepared, [2])

    expected =
      ~s/#Xandra.Batch<[type: :logged, / <>
        ~s/queries: [{"INSERT INTO users (id, name) VALUES (1, 'Marge')", []}, / <>
        ~s/{#Xandra.Prepared<"DELETE FROM users WHERE id = ?">, [2]}]]>/

    assert inspect(batch) == expected
  end
end
