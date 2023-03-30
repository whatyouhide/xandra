defmodule WarningTest do
  use XandraTest.IntegrationCase, async: true

  import Xandra.TestHelper, only: [{:mirror_telemetry_event, 1}]

  alias Xandra.Batch

  @moduletag :skip_for_native_protocol_v3
  @moduletag :cassandra_specific

  setup_all %{keyspace: keyspace, setup_conn: setup_conn} do
    Xandra.execute!(setup_conn, "USE #{keyspace}")
    Xandra.execute!(setup_conn, "CREATE TABLE fruits (id int, name text, PRIMARY KEY (id))")
    :ok
  end

  setup %{conn: conn} do
    Xandra.execute!(conn, "TRUNCATE fruits")
    :ok
  end

  test "batch of type \"unlogged\" producing warning", %{conn: conn, keyspace: keyspace} do
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

    mirror_telemetry_event([:xandra, :server_warnings])
    Xandra.execute!(conn, batch)

    result = for %{"name" => name} <- Xandra.execute!(conn, "SELECT name FROM fruits"), do: name
    assert Enum.sort(result) == fruit_names

    assert_receive {:telemetry_event, [:xandra, :server_warnings], measurements, metadata}
    assert %{warnings: [warning]} = measurements
    assert warning =~ "Unlogged batch covering 11 partitions"
    assert metadata.host == '127.0.0.1'
    assert metadata.port == 9042
    assert metadata.current_keyspace == keyspace
    assert inspect(metadata.query) == inspect(batch)
  end

  # This test is broken when using native protocol v3 on C* 4.0.
  # See: https://github.com/lexhide/xandra/issues/218
  @tag :skip_for_native_protocol_v4
  test "regression for crash after warning", %{keyspace: keyspace, start_options: start_options} do
    start_options = Keyword.put(start_options, :protocol_version, :v3)
    conn = start_supervised!({Xandra, start_options})

    Xandra.execute!(conn, "USE #{keyspace}")

    Xandra.execute!(conn, """
    CREATE TABLE dimensions (id int, dimension text, PRIMARY KEY (id, dimension))
    """)

    ids = Enum.take_random(1..100_000, 10)
    ids_as_params = Enum.map_join(ids, ", ", fn _ -> "?" end)

    query = """
    SELECT * FROM dimensions
    WHERE id IN (#{ids_as_params})
    GROUP BY id, dimension;
    """

    Xandra.execute!(conn, query, Enum.map(ids, &{"int", &1}))
  end
end
