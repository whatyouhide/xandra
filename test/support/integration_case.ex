defmodule XandraTest.IntegrationCase do
  use ExUnit.CaseTemplate

  using options do
    quote bind_quoted: [
            start_options: Keyword.get(options, :start_options, []),
            case_template: __MODULE__
          ] do
      setup_all do
        keyspace = unquote(case_template).gen_keyspace(__MODULE__)

        start_options =
          Keyword.merge(
            unquote(case_template).default_start_options(),
            unquote(start_options)
          )

        conn = Xandra.TestHelper.start_link_supervised!({Xandra, start_options})

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

  @spec default_start_options() :: keyword()
  def default_start_options do
    options = [show_sensitive_data_on_connection_error: true]

    case System.get_env("CASSANDRA_NATIVE_PROTOCOL", "") do
      "v3" -> Keyword.put(options, :protocol_version, :v3)
      "v4" -> Keyword.put(options, :protocol_version, :v4)
      "v5" -> Keyword.put(options, :protocol_version, :v5)
      "" -> options
    end
  end

  @spec gen_keyspace(module()) :: String.t()
  def gen_keyspace(module) when is_atom(module) do
    suffix =
      inspect(module)
      |> String.replace(".", "")
      |> String.downcase()

    "xandra_test_" <> suffix
  end

  @spec setup_keyspace(Xandra.conn(), String.t()) :: :ok
  def setup_keyspace(conn, keyspace) do
    Xandra.execute!(conn, "DROP KEYSPACE IF EXISTS #{keyspace}")

    statement = """
    CREATE KEYSPACE #{keyspace}
    WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """

    Xandra.execute!(conn, statement)
    :ok
  end

  @spec drop_keyspace(String.t(), keyword()) :: :ok
  def drop_keyspace(keyspace, start_options) do
    # Cannot use start_supervised! here because this is called from on_exit.
    {:ok, conn} = Xandra.start_link(start_options)
    Xandra.execute!(conn, "DROP KEYSPACE IF EXISTS #{keyspace}")
    :ok = GenServer.stop(conn)
  end

  @spec protocol_version() :: :v3 | :v4 | :v5
  def protocol_version do
    default_start_options()[:protocol_version]
  end
end
