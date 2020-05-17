defmodule WarningTest do
  use XandraTest.IntegrationCase, async: true, start_options: [protocol_version: :v4]

  alias Xandra.Batch

  # Technically, this is supported by Cosmos DB, but this test cannot trigger a warning like this.
  @moduletag :cosmosdb_unsupported

  setup_all %{keyspace: keyspace, start_options: start_options} do
    {:ok, conn} = Xandra.start_link(start_options)
    Xandra.execute!(conn, "USE #{keyspace}")

    statement = "CREATE TABLE fruits (id int, name text, PRIMARY KEY (id))"
    Xandra.execute!(conn, statement)

    :ok
  end

  setup %{conn: conn} do
    Xandra.execute!(conn, "TRUNCATE fruits")
    :ok
  end

  test "batch of type \"unlogged\" producing warning", %{conn: conn} do
    # Batches spanning more partitions than "unlogged_batch_across_partitions_warn_threshold"
    # (default: 10) generate a warning. Right now we don't use the warning but the warning
    # causes the payload to be different so we need to test that we're able to decode this
    # just fine anyways.
    fruit_names = [
      "Apple",
      "Apricot",
      "Avocado",
      "Banana",
      "Cherry",
      "Orange",
      "Papaya",
      "Passion fruit",
      "Peach",
      "Pear",
      "Watermelon"
    ]

    batch =
      Enum.reduce(Enum.with_index(fruit_names, 1), Batch.new(:unlogged), fn {name, index}, acc ->
        Batch.add(acc, "INSERT INTO fruits (id, name) VALUES (#{index}, '#{name}')")
      end)

    Xandra.execute!(conn, batch)

    result = for %{"name" => name} <- Xandra.execute!(conn, "SELECT name FROM fruits"), do: name
    assert Enum.sort(result) == fruit_names
  end
end
