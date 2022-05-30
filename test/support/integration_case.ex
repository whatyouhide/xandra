defmodule XandraTest.IntegrationCase do
  use ExUnit.CaseTemplate

  using options do
    quote bind_quoted: [
            start_options: Keyword.get(options, :start_options, []),
            case_template: __MODULE__
          ] do
      setup_all do
        keyspace = XandraTest.IntegrationCase.gen_keyspace(__MODULE__)

        start_options =
          Keyword.merge(
            XandraTest.IntegrationCase.default_start_options(),
            unquote(start_options)
          )

        {:ok, conn} = Xandra.start_link(start_options)

        unquote(case_template).setup_keyspace(conn, keyspace)

        on_exit(fn ->
          unquote(case_template).drop_keyspace(keyspace, start_options)
        end)

        %{keyspace: keyspace, start_options: start_options, setup_conn: conn}
      end
    end
  end

  setup %{keyspace: keyspace, start_options: start_options} do
    conn = start_supervised!({Xandra, start_options}, id: "setup_conn_#{keyspace}")
    Xandra.execute!(conn, "USE #{keyspace}")
    %{conn: conn}
  end

  def default_start_options do
    options = [show_sensitive_data_on_connection_error: true]

    case System.get_env("CASSANDRA_NATIVE_PROTOCOL", "") do
      "v3" -> Keyword.put(options, :protocol_version, :v3)
      "v4" -> Keyword.put(options, :protocol_version, :v4)
      "v5" -> Keyword.put(options, :protocol_version, :v5)
      "" -> options
    end
  end

  def gen_keyspace(module) when is_atom(module) do
    suffix =
      inspect(module)
      |> String.replace(".", "")
      |> String.downcase()

    "xandra_test_" <> suffix
  end

  def setup_keyspace(conn, keyspace) do
    Xandra.execute!(conn, "DROP KEYSPACE IF EXISTS #{keyspace}")

    statement = """
    CREATE KEYSPACE #{keyspace}
    WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """

    Xandra.execute!(conn, statement)
  end

  def drop_keyspace(keyspace, start_options) do
    {:ok, conn} = Xandra.start_link(start_options)
    Xandra.execute!(conn, "DROP KEYSPACE IF EXISTS #{keyspace}")
    :ok = GenServer.stop(conn)
  end

  def protocol_version do
    default_start_options()[:protocol_version]
  end
end
