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

cassandra_version = System.get_env("CASSANDRA_VERSION", "")
protocol_version = XandraTest.IntegrationCase.protocol_version()

ex_unit_start_opts =
  cond do
    # C* 2.x doesn't support native protocol v4+, so we skip those.
    String.starts_with?(cassandra_version, "2") ->
      [exclude: [requires_native_protocol: :v4]]

    # We first exclude all of the tests that require a specific protocol, and
    # then we re-include all of the ones that require the specific
    # protocol we forced.
    protocol_version ->
      [
        exclude: [:requires_native_protocol],
        include: [requires_native_protocol: protocol_version]
      ]

    true ->
      []
  end

ExUnit.start(ex_unit_start_opts)
