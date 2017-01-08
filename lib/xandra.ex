defmodule Xandra do
  alias __MODULE__.{Connection, Prepared, Query, Error}

  def start_link(opts \\ []) do
    opts = Keyword.put_new(opts, :host, "127.0.0.1")
    DBConnection.start_link(Connection, opts)
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

  def execute(conn, %query_kind{} = query, params, opts) when query_kind in [Query, Prepared] do
    with {:ok, %Error{} = error} <- DBConnection.execute(conn, query, params, opts) do
      {:error, error}
    end
  end

  def prepare_execute(conn, statement, params, opts \\ []) when is_binary(statement) do
    DBConnection.prepare_execute(conn, %Prepared{statement: statement}, params, opts)
  end
end
