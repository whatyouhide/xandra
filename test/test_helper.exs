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
        include: [requires_native_protocol: Atom.to_string(protocol_version)]
      ]

    # If not native protocol was specified, we default to negotiating, which
    # picks the latest. We should skip tests that *require* older versions.
    is_nil(protocol_version) ->
      [exclude: [requires_native_protocol: :v3]]

    true ->
      []
  end

# Some tests are broken when using native protocol v3 on C* 4.0.
# See: https://github.com/lexhide/xandra/issues/218
# TODO: Remove this once we run tests on C* 4.1 (once it's released).
cassandra_version_with_bug? =
  cassandra_version == (_default_is_4 = "") or
    String.starts_with?(cassandra_version, "4")

ex_unit_start_opts =
  if cassandra_version_with_bug? and protocol_version == :v3 do
    Keyword.update(
      ex_unit_start_opts,
      :exclude,
      [:skip_for_cassandra4_with_protocol_v3],
      &(&1 ++ [:skip_for_cassandra4_with_protocol_v3])
    )
  else
    ex_unit_start_opts
  end

require Logger
Logger.info("Running test suite with options: #{inspect(ex_unit_start_opts)}")
ExUnit.start(ex_unit_start_opts)
