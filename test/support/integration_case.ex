defmodule XandraTest.IntegrationCase do
  use ExUnit.CaseTemplate

  import ExUnit.Assertions

  using options do
    quote bind_quoted: [
            start_options: Keyword.get(options, :start_options, []),
            case_template: __MODULE__,
            generate_keyspace: Keyword.get(options, :generate_keyspace, true)
          ] do
      setup_all do
        start_options =
          Keyword.merge(
            unquote(case_template).default_start_options(),
            unquote(start_options)
          )

        if unquote(generate_keyspace) do
          keyspace = unquote(case_template).gen_keyspace(__MODULE__)
          conn = start_link_supervised!({Xandra, start_options})

          unquote(case_template).setup_keyspace(conn, keyspace)

          on_exit(fn ->
            unquote(case_template).drop_keyspace(keyspace, start_options)
          end)

          %{keyspace: keyspace, start_options: start_options, setup_conn: conn}
        else
          %{start_options: start_options}
        end
      end
    end
  end

  setup context do
    start_conn? = Map.get(context, :start_conn, true)

    case context do
      %{keyspace: keyspace, start_options: start_options} when start_conn? ->
        start_options = Keyword.put(start_options, :keyspace, keyspace)
        conn = start_supervised!({Xandra, start_options}, id: "setup_conn_#{keyspace}")
        %{conn: conn}

      _other ->
        %{}
    end
  end

  @spec default_start_options() :: keyword()
  def default_start_options do
    options = [nodes: ["127.0.0.1:#{port()}"]]

    if protocol_version = protocol_version() do
      Keyword.put(options, :protocol_version, protocol_version)
    else
      options
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
    :ok = Xandra.stop(conn)
  end

  @spec protocol_version() :: :v3 | :v4 | :v5 | nil
  def protocol_version do
    case System.get_env("CASSANDRA_NATIVE_PROTOCOL", "") do
      "v3" -> :v3
      "v4" -> :v4
      "v5" -> :v5
      "" -> nil
      other -> flunk("Unsupported value for the CASSANDRA_NATIVE_PROTOCOL env variable: #{other}")
    end
  end

  @spec port() :: :inet.port_number()
  def port do
    "CASSANDRA_PORT"
    |> System.get_env("9052")
    |> String.to_integer()
  end

  @spec port_with_auth() :: :inet.port_number()
  def port_with_auth do
    "CASSANDRA_WITH_AUTH_PORT"
    |> System.get_env("9053")
    |> String.to_integer()
  end

  @spec port_with_toxiproxy() :: :inet.port_number()
  def port_with_toxiproxy do
    String.to_integer("1#{port()}")
  end

  @spec cassandra_port_with_ssl() :: :inet.port_number()
  def cassandra_port_with_ssl, do: 9152

  @spec toxiproxy_proxy() :: :xandra_test_cassandra | :xandra_test_scylla
  def toxiproxy_proxy do
    case port() do
      9052 -> :xandra_test_cassandra
      9062 -> :xandra_test_scylla
    end
  end
end
