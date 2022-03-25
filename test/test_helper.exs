defmodule XandraTest.IntegrationCase do
  use ExUnit.CaseTemplate

  protocol_version =
    case System.get_env("CASSANDRA_NATIVE_PROTOCOL") || "v3" do
      "v3" -> :v3
      "v4" -> :v4
    end

  @default_start_options [
    protocol_version: protocol_version,
    show_sensitive_data_on_connection_error: true
  ]

  using options do
    start_options = Keyword.get(options, :start_options, [])

    quote bind_quoted: [
            start_options: start_options,
            case_template: __MODULE__,
            default_start_options: @default_start_options
          ] do
      setup_all do
        module_suffix =
          inspect(__MODULE__)
          |> String.replace(".", "")
          |> String.downcase()

        keyspace = "xandra_test_" <> module_suffix

        start_options = Keyword.merge(unquote(default_start_options), unquote(start_options))
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

  def protocol_version, do: unquote(protocol_version)
end

ExUnit.start(exclude: [skip_for_native_protocol: XandraTest.IntegrationCase.protocol_version()])
