defmodule XandraTest.IntegrationCase do
  use ExUnit.CaseTemplate

  @default_start_options [
    show_sensitive_data_on_connection_error: true
  ]

  @default_start_options (case System.get_env("CASSANDRA_NATIVE_PROTOCOL", "") do
                            "v3" -> Keyword.put(@default_start_options, :protocol_version, :v3)
                            "v4" -> Keyword.put(@default_start_options, :protocol_version, :v4)
                            "" -> @default_start_options
                          end)

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

  def protocol_version, do: unquote(@default_start_options[:protocol_version])
end

Logger.configure(level: :info)

cassandra_version = System.get_env("CASSANDRA_VERSION", "4")
protocol_version = XandraTest.IntegrationCase.protocol_version()

max_supported_protocol =
  if String.starts_with?(cassandra_version, "2") do
    :v3
  else
    Xandra.Frame.max_supported_protocol()
  end

native_protocol_excludes =
  case protocol_version || max_supported_protocol do
    :v3 -> [min_native_protocol: :v4]
    :v4 -> [max_native_protocol: :v3]
  end

ExUnit.start(exclude: native_protocol_excludes)
