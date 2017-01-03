defmodule Xandra do
  alias __MODULE__.{Connection, Prepared, Query, Error}

  @xandra_opts [
    host: "127.0.0.1",
    port: 9042,
  ]

  def start_link(xandra_opts \\ [], db_connection_opts \\ [])
      when is_list(xandra_opts) and is_list(db_connection_opts) do
    xandra_opts =
      @xandra_opts
      |> Keyword.merge(xandra_opts)
      |> validate_xandra_opts()
    DBConnection.start_link(Connection, xandra_opts ++ db_connection_opts)
  end

  def stream!(conn, query, params, opts \\ [])

  def stream!(conn, statement, params, opts) when is_binary(statement) do
    with {:ok, query} <- prepare(conn, statement, opts) do
      stream!(conn, query, params, opts)
    end
  end

  def stream!(conn, %Query{} = query, params, opts) do
    %Xandra.Stream{conn: conn, query: query, params: params, opts: opts}
  end

  def prepare(conn, statement, opts \\ []) when is_binary(statement) do
    DBConnection.prepare(conn, %Prepared{statement: statement}, opts)
  end

  def execute(conn, statement, params, opts \\ [])

  def execute(conn, statement, params, opts) when is_binary(statement) do
    execute(conn, %Query{statement: statement}, params, opts)
  end

  def execute(conn, %kind{} = query, params, opts) when kind in [Query, Prepared] do
    with {:ok, %Error{} = error} <- DBConnection.execute(conn, query, params, opts) do
      {:error, error}
    end
  end

  def prepare_execute(conn, statement, params, opts \\ []) when is_binary(statement) do
    DBConnection.prepare_execute(conn, %Prepared{statement: statement}, params, opts)
  end

  defp validate_xandra_opts(opts) do
    Enum.map(opts, fn
      {:host, host} ->
        {:host, to_charlist(host)}
      {:port, port} ->
        if is_integer(port) do
          {:port, port}
        else
          raise ArgumentError,
            "expected an integer as the value of the :port option, got: #{inspect(port)}"
        end
    end)
  end
end
