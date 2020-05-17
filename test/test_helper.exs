ExUnit.start()

defmodule XandraTest.IntegrationCase do
  use ExUnit.CaseTemplate

  protocol_version =
    case System.get_env("CASSANDRA_NATIVE_PROTOCOL") || "v3" do
      "v3" -> :v3
      "v4" -> :v4
    end

  @is_cosmosdb System.get_env("CASSANDRA_IS_COSMOSDB") == "true"

  if @is_cosmosdb do
    @default_start_options [
      nodes: ["localhost:10350"],
      protocol_version: protocol_version,
      show_sensitive_data_on_connection_error: true,
      encryption: true,
      authentication:
        {Xandra.Authenticator.Password,
         username: "localhost",
         password:
           "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="}
    ]
  else
    @default_start_options [
      protocol_version: protocol_version,
      show_sensitive_data_on_connection_error: true
    ]
  end

  using options do
    start_options = Keyword.get(options, :start_options, [])

    quote bind_quoted: [
            start_options: start_options,
            case_template: __MODULE__,
            is_cosmosdb: @is_cosmosdb,
            default_start_options: @default_start_options
          ] do
      setup_all do
        keyspace = "xandra_test_" <> String.replace(inspect(__MODULE__), ".", "")

        start_options = Keyword.merge(unquote(default_start_options), unquote(start_options))
        case_template = unquote(case_template)

        case_template.setup_keyspace(keyspace, start_options)

        on_exit(fn ->
          case_template.drop_keyspace(keyspace, start_options)
        end)

        %{keyspace: keyspace, start_options: start_options, is_cosmosdb: unquote(is_cosmosdb)}
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
