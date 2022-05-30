defmodule UseTest do
  use XandraTest.IntegrationCase, async: true

  setup_all %{keyspace: keyspace, start_options: start_options} do
    {:ok, conn} = Xandra.start_link(start_options)

    other_keyspace = keyspace <> "_2"
    Xandra.execute!(conn, "DROP KEYSPACE IF EXISTS #{other_keyspace}")

    statement = """
    CREATE KEYSPACE #{other_keyspace}
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """

    Xandra.execute!(conn, statement)

    keyspaces = [keyspace, other_keyspace]

    for keyspace <- keyspaces do
      statement = "CREATE TABLE #{keyspace}.whoami (whoami text, PRIMARY KEY (whoami))"

      Xandra.execute!(conn, statement)

      statement = "INSERT INTO #{keyspace}.whoami (whoami) VALUES (:whoami)"
      params = %{"whoami" => {"text", keyspace}}
      Xandra.execute!(conn, statement, params)
    end

    on_exit(fn ->
      {:ok, conn} = Xandra.start_link(start_options)

      for keyspace <- keyspaces do
        Xandra.execute!(conn, "DROP KEYSPACE IF EXISTS #{keyspace}")
      end
    end)

    [keyspaces: keyspaces]
  end

  test "use statement followed by an execute statement", %{conn: conn, keyspaces: keyspaces} do
    for keyspace <- keyspaces do
      statement = "USE #{keyspace}"

      assert %Xandra.SetKeyspace{keyspace: ^keyspace} = Xandra.execute!(conn, statement)

      statement = "SELECT whoami FROM whoami"

      page = Xandra.execute!(conn, statement, %{})

      assert Enum.to_list(page) == [%{"whoami" => keyspace}]
    end
  end

  # TODO: unskip this once we can test on C* 4.1+.
  # This test is skipped because the prepared statement without an explicit keyspace generates a
  # warning, and for protocol v3 on C* 4 there is a bug with warnings.
  @tag :skip_for_native_protocol_v3
  test "use statement followed by a prepared statement", %{conn: conn, keyspaces: keyspaces} do
    for keyspace <- keyspaces do
      statement = "USE #{keyspace}"

      assert %Xandra.SetKeyspace{keyspace: ^keyspace} = Xandra.execute!(conn, statement)

      statement = "SELECT whoami FROM whoami"
      prepared = Xandra.prepare!(conn, statement)

      page = Xandra.execute!(conn, prepared)
      assert Enum.to_list(page) == [%{"whoami" => keyspace}]
    end
  end
end
