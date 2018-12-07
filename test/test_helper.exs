if System.get_env("AUTHENTICATION") == "true" do
  ExUnit.start(exclude: [:test], include: [:authentication])
else
  ExUnit.start()
end

defmodule XandraTest.IntegrationCase do
  use ExUnit.CaseTemplate

  using options do
    start_options = Keyword.get(options, :start_options, [])

    quote bind_quoted: [start_options: start_options, case_template: __MODULE__] do
      setup_all do
        keyspace = "xandra_test_" <> String.replace(inspect(__MODULE__), ".", "")
        start_options = unquote(start_options)
        case_template = unquote(case_template)

        case_template.setup_keyspace(keyspace, start_options)

        on_exit(fn ->
          case_template.drop_keyspace(keyspace, start_options)
        end)

        %{keyspace: keyspace, start_options: start_options}
      end
    end
  end

  setup %{keyspace: keyspace, start_options: start_options} do
    {:ok, conn} = Xandra.start_link(start_options)
    Xandra.execute!(conn, "USE #{keyspace}")
    %{conn: conn}
  end

  def setup_keyspace(keyspace, start_options) do
    {:ok, conn} = Xandra.start_link(start_options)
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
  end
end
