defmodule Xandra do
  alias __MODULE__.{Batch, Connection, Error, Prepared, Rows, Simple, Stream}

  @type statement :: String.t
  @type values :: list | map
  @type error :: Error.t | Connection.Error.t
  @type result :: Void.t | Rows.t | SetKeyspace.t | SchemaChange.t
  @type conn :: DBConnection.conn

  @default_options [
    host: "127.0.0.1",
    port: 9042,
  ]

  @spec start_link(Keyword.t) :: GenServer.on_start
  def start_link(options \\ []) when is_list(options) do
    options =
      @default_options
      |> Keyword.merge(options)
      |> validate_options()
      |> Keyword.put(:prepared_cache, Prepared.Cache.new)
    DBConnection.start_link(Connection, options)
  end

  @spec stream!(conn, statement | Prepared.t, values, Keyword.t) :: Enumerable.t
  def stream!(conn, query, params, options \\ [])

  def stream!(conn, statement, params, options) when is_binary(statement) do
    %Stream{conn: conn, query: statement, params: params, options: options}
  end

  def stream!(conn, %Prepared{} = prepared, params, options) do
    %Stream{conn: conn, query: prepared, params: params, options: options}
  end

  @spec prepare(conn, statement, Keyword.t) :: {:ok, Prepared.t} | {:error, error}
  def prepare(conn, statement, options \\ []) when is_binary(statement) do
    DBConnection.prepare(conn, %Prepared{statement: statement}, options)
  end

  @spec prepare!(conn, statement, Keyword.t) :: Prepared.t | no_return
  def prepare!(conn, statement, options \\ []) do
    case prepare(conn, statement, options) do
      {:ok, result} -> result
      {:error, exception} -> raise(exception)
    end
  end

  @spec execute(conn, statement | Prepared.t, values) :: {:ok, result} | {:error, error}
  @spec execute(conn, Batch.t, Keyword.t) :: {:ok, Void.t} | {:error, error}
  def execute(conn, query, params_or_options \\ [])

  def execute(conn, statement, params) when is_binary(statement) do
    execute(conn, statement, params, _options = [])
  end

  def execute(conn, %Prepared{} = prepared, params) do
    execute(conn, prepared, params, _options = [])
  end

  def execute(conn, %Batch{} = batch, options) when is_list(options) do
    with {:ok, %Error{} = error} <- DBConnection.execute(conn, batch, :no_params, options),
         do: {:error, error}
  end

  @spec execute(conn, statement | Prepared.t, values, Keyword.t) :: {:ok, result} | {:error, error}
  def execute(conn, query, params, options)

  def execute(conn, statement, params, options) when is_binary(statement) do
    options = put_paging_state(options)
    query = %Simple{statement: statement}
    with {:ok, %Error{} = error} <- DBConnection.execute(conn, query, params, options) do
      {:error, error}
    end
  end

  def execute(conn, %Prepared{} = prepared, params, options) do
    options = put_paging_state(options)
    case DBConnection.execute(conn, prepared, params, options) do
      {:ok, %Error{reason: :unprepared}} ->
        run_prepare_execute(conn, prepared, params, Keyword.put(options, :force, true))
      {:ok, %Error{} = error} ->
        {:error, error}
      other ->
        other
    end
  end

  @spec execute(conn, statement | Prepared.t, values) :: result | no_return
  @spec execute(conn, Batch.t, Keyword.t) :: Void.t | no_return
  def execute!(conn, query, params_or_options \\ []) do
    case execute(conn, query, params_or_options) do
      {:ok, result} -> result
      {:error, exception} -> raise(exception)
    end
  end

  @spec execute!(conn, statement | Prepared.t, values, Keyword.t) :: result | no_return
  def execute!(conn, query, params, options) do
    case execute(conn, query, params, options) do
      {:ok, result} -> result
      {:error, exception} -> raise(exception)
    end
  end

  defp run_prepare_execute(conn, %Prepared{} = prepared, params, options) do
    with {:ok, _prepared, %Error{} = error} <- DBConnection.prepare_execute(conn, prepared, params, options) do
      {:error, error}
    end
  end

  defp put_paging_state(options) do
    case Keyword.pop(options, :cursor) do
      {%Rows{paging_state: paging_state}, options} ->
        Keyword.put(options, :paging_state, paging_state)
      {nil, options} ->
        options
    end
  end

  defp validate_options(options) do
    Enum.map(options, fn
      {:host, host} ->
        if is_binary(host) do
          {:host, String.to_charlist(host)}
        else
          raise ArgumentError, "expected a string as the value of the :host option, got: #{inspect(host)}"
        end
      {:port, port} ->
        if is_integer(port) do
          {:port, port}
        else
          raise ArgumentError, "expected an integer as the value of the :port option, got: #{inspect(port)}"
        end
      {_key, _value} = option ->
        option
    end)
  end
end
