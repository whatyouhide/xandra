defmodule Xandra do
  alias __MODULE__.{Connection, Query}

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
    DBConnection.prepare(conn, %Query{statement: statement}, opts)
  end

  def execute(conn, statement, params, opts \\ [])

  def execute(conn, statement, params, opts) when is_binary(statement) do
    execute(conn, %Query{statement: statement}, params, opts)
  end

  def execute(conn, %Query{} = query, params, opts) do
    DBConnection.execute(conn, query, params, opts)
  end

  def prepare_execute(conn, statement, params, opts \\ []) when is_binary(statement) do
    DBConnection.prepare_execute(conn, %Query{statement: statement}, params, opts)
  end
end
