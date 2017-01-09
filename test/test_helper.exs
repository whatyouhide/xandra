ExUnit.start()

defmodule XandraTest.IntegrationCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      setup_all do
        keyspace = "xandra_test_" <> Base.encode16(:crypto.strong_rand_bytes(16))
        unquote(__MODULE__).setup_keyspace(keyspace)

        %{keyspace: keyspace}
      end
    end
  end

  setup %{keyspace: keyspace} do
    {:ok, conn} = Xandra.start_link()
    {:ok, _void} = Xandra.execute(conn, "USE #{keyspace}", [])
    %{conn: conn}
  end

  def setup_keyspace(keyspace) do
    {:ok, conn} = Xandra.start_link()
    {:ok, _void} = Xandra.execute(conn, "DROP KEYSPACE IF EXISTS #{keyspace}", [])
    statement = """
    CREATE KEYSPACE #{keyspace}
    WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """
    {:ok, _void} = Xandra.execute(conn, statement, [])
  end
end
