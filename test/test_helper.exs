ExUnit.start()

defmodule XandraTest.IntegrationCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      setup_all do
        keyspace = "xandra_test_" <> String.replace(inspect(__MODULE__), ".", "")
        unquote(__MODULE__).setup_keyspace(keyspace)

        on_exit(fn ->
          unquote(__MODULE__).drop_keyspace(keyspace)
        end)

        %{keyspace: keyspace}
      end
    end
  end

  setup %{keyspace: keyspace} do
    {:ok, conn} = Xandra.start_link()
    Xandra.execute!(conn, "USE #{keyspace}", [])
    %{conn: conn}
  end

  def setup_keyspace(keyspace) do
    {:ok, conn} = Xandra.start_link()
    Xandra.execute!(conn, "DROP KEYSPACE IF EXISTS #{keyspace}", [])
    statement = """
    CREATE KEYSPACE #{keyspace}
    WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """
    Xandra.execute!(conn, statement, [])
  end

  def drop_keyspace(keyspace) do
    {:ok, conn} = Xandra.start_link()
    Xandra.execute!(conn, "DROP KEYSPACE IF EXISTS #{keyspace}", [])
  end
end
