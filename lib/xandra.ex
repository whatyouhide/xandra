defmodule Xandra do
  @moduledoc """
  This module provides the main API to interface with Cassandra.

  This module handles the connection to Cassandra, queries, connection pooling,
  connection backoff, logging, and more. Many of these features are provided by
  the [`DBConnection`](https://hex.pm/packages/db_connection) library, which
  Xandra is built on top of.

  ## Errors

  Many of the functions in this module (whose names don't end with a `!`)
  return values in the form `{:ok, result}` or `{:error, error}`. While `result`
  varies based on the specific function, `error` is always one of the following:

    * a `Xandra.Error` struct: such structs represent errors returned by
      Cassandra. When such an error is returned, it means that communicating
      with the Cassandra server was successful, but the server returned an
      error. Examples of these errors are syntax errors in queries, non-existent
      tables, and so on. See `Xandra.Error` for more information.

    * a `Xandra.ConnectionError` struct: such structs represent errors in the
      communication with the Cassandra server. For example, if the Cassandra
      server dies while the connection is waiting for a response from the
      server, a `Xandra.ConnectionError` error will be returned. See
      `Xandra.ConnectionError` for more information.

  ## Parameters, encoding, and types

  Xandra supports parameterized queries (queries that specify "parameter" values
  through `?` or `:named_value`):

      SELECT * FROM users WHERE name = ? AND email = ?
      SELECT * FROM users WHERE name = :name AND email = :email

  When a query has positional parameters, parameters can be passed as a list to
  functions like `execute/4`: in this case, a parameter in a given position in
  the list will be used as the `?` in the corresponding position in the
  query. When a query has named parameters, parameters are passed as a map with
  string keys representing each parameter's name and values representing the
  corresponding parameter's value.

  ### Types

  Cassandra supports many types of values, and some types have "shades" that
  cannot be represented by Elixir types. For example, in Cassandra an integer
  could be a "bigint" (a 64 bit integer), an "int" (a 32 bit integer), a
  "smallint" (a 16 bit integer), or others; in Elixir, however, integers are
  just integers (with varying size to be precise), so it is impossible to
  univocally map Elixir integers to a specific Cassandra integer type. For this
  reason, when executing simple parameterized queries (statements) it is
  necessary to explicitly specify the type of each value.

  To specify the type of a value, that value needs to be provided as a
  two-element tuple where the first element is the value's type and the second
  element is the value itself. Types are expressed with the same syntax used in
  CQL: for example, 16-bit integers are represented as `"smallint"`, while maps
  of strings to booleans are represented as `"map<text, boolean>"`.

      # Using a list of parameters:
      statement = "INSERT INTO species (name, properties) VALUES (?, ?)"
      Xandra.execute(conn, statement, [
        {"text", "human"},
        {"map<text, boolean>", %{"legs" => true, "arms" => true, "tail" => false}},
      ])

      # Using a map of parameters:
      statement = "INSERT INTO species (name, properties) VALUES (:name, :properties)"
      Xandra.execute(conn, statement, %{
        "name" => {"text", "human"},
        "properties" => {"map<text, boolean>", %{"legs" => true, "arms" => true, "tail" => false}},
      })

  You only need to specify types for simple queries (statements): when using
  prepared queries, the type information of each parameter of the query is
  encoded in the prepared query itself.

      # Using a map of parameters:
      prepared = Xandra.prepare!(conn, "INSERT INTO species (name, properties) VALUES (:name, :properties)")
      Xandra.execute(conn, prepared, %{
        "name" => "human",
        "properties" => %{"legs" => true, "arms" => true, "tail" => false},
      })

  #### User-defined types

  Xandra supports user-defined types (UDTs). A UDT can be inserted as a map with
  string fields. For example, consider having created the following UDTs:

      CREATE TYPE full_name (first_name text, last_name text)
      CREATE TYPE profile (username text, full_name frozen<full_name>)

  and having the following table:

      CREATE TABLE users (id int PRIMARY KEY, profile frozen<profile>)

  Inserting rows will look something like this:

      prepared_insert = Xandra.prepare!(conn, "INSERT INTO users (id, profile) VALUES (?, ?)")

      profile = %{
        "username" => "bperry",
        "full_name" => %{"first_name" => "Britta", "last_name" => "Perry"},
      }
      Xandra.execute!(conn, prepared_insert, [_id = 1, profile])

  Note that inserting UDTs is only supported on prepared queries.

  When retrieved, UDTs are once again represented as maps with string
  keys. Retrieving the row inserted above would look like this:

      %{"profile" => profile} = conn |> Xandra.execute!("SELECT id, profile FROM users") |> Enum.fetch!(0)
      profile
      #=> %{"username" => "bperry", "full_name" => %{"first_name" => "Britta", "last_name" => "Perry"}}

  ## Reconnections

  Thanks to the `DBConnection` library, Xandra is able to handle connection
  losses and to automatically reconnect to Cassandra. By default, reconnections
  are retried at exponentially increasing randomized intervals, but backoff can
  be configured through a subset of the options accepted by
  `start_link/2`. These options are described in the documentation for
  `DBConnection.start_link/2`.

  ## Clustering

  Xandra supports connecting to multiple nodes in a Cassandra cluster and
  executing queries on different nodes based on load balancing strategies. See
  the documentation for `Xandra.Cluster` for more information.

  ## Authentication

  Xandra supports Cassandra authentication. See the documentation for
  `Xandra.Authenticator` for more information.

  ## Retrying failed queries

  Xandra takes a customizable and extensible approach to retrying failed queries
  through "retry strategies" that encapsulate the logic for retrying
  queries. See `Xandra.RetryStrategy` for documentation on retry strategies.

  ## Compression

  Xandra supports compression. To inform the Cassandra server that the
  connections you start should use compression for data transmitted to and from
  the server, you can pass the `:compressor` option to `start_link/1`; this
  option should be a module that implements the `Xandra.Compressor`
  behaviour. After this, all compressed data that Cassandra sends to the
  connection will be decompressed using this behaviour module.

  To compress outgoing data (such as when issuing or preparing queries), the
  `:compressor` option should be specified explicitly. When it's specified, the
  given module will be used to compress data. If no `:compressor` option is
  passed, the outgoing data will not be compressed.
  """

  alias __MODULE__.{Batch, Connection, ConnectionError, Error, Prepared, Page, PageStream, Simple}

  @type statement :: String.t()
  @type values :: list | map
  @type error :: Error.t() | ConnectionError.t()
  @type result :: Xandra.Void.t() | Page.t() | Xandra.SetKeyspace.t() | Xandra.SchemaChange.t()
  @type conn :: DBConnection.conn()

  @type xandra_start_option ::
          {:nodes, [String.t()]}
          | {:compressor, module}
          | {:authentication, {module, Keyword.t()}}
          | {:atom_keys, boolean}

  @type db_connection_start_option :: {atom(), any}
  @type start_option :: xandra_start_option | db_connection_start_option
  @type start_options :: [start_option]

  @default_port 9042
  @default_start_options [
    nodes: ["127.0.0.1"],
    idle_timeout: 30_000
  ]

  @doc """
  Starts a new connection or pool of connections to Cassandra.

  This function starts a new connection or pool of connections to the provided
  Cassandra server. `options` is a list of both Xandra-specific options, as well
  as `DBConnection` options.

  ## Options

  These are the Xandra-specific options supported by this function:

    * `:nodes` - (list of strings) the Cassandra nodes to connect to. Each node
      in the list has to be in the form `"ADDRESS:PORT"` or in the form
      `"ADDRESS"`: if the latter is used, the default port (`#{@default_port}`)
      will be used for that node. Defaults to `["127.0.0.1"]`. This option must
      contain only one node unless the `:pool` option is set to
      `Xandra.Cluster`; see the documentation for `Xandra.Cluster` for more
      information.

    * `:compressor` - (module) the compressor module to use for compressing and
      decompressing data. See the "Compression" section in the module
      documentation. By default this option is not present.

    * `:authentication` - (tuple) a two-element tuple: the authenticator
      module to use for authentication and its supported options. See the
      "Authentication" section in the module documentation.

    * `:atom_keys` - (boolean) whether or not results of and parameters to
      `execute/4` will have atom keys. If `true`, the result maps will have
      column names returned as atoms rather than as strings. Additionally,
      maps that represent named parameters will need atom keys. Defaults to `false`.

  The rest of the options are forwarded to `DBConnection.start_link/2`. For
  example, to start a pool of connections to Cassandra, the `:pool` option can
  be used:

      Xandra.start_link(pool: DBConnection.Poolboy)

  Note that this requires the `poolboy` dependency to be specified in your
  application. The following options have default values that are different from
  the default values provided by `DBConnection`:

    * `:idle_timeout` - defaults to `30_000` (30 seconds)

  ## Examples

      # Start a connection:
      {:ok, conn} = Xandra.start_link()

      # Start a connection and register it under a name:
      {:ok, _conn} = Xandra.start_link(name: :xandra)

      # Start a named pool of connections:
      {:ok, _pool} = Xandra.start_link(name: :xandra_pool, pool: DBConnection.Poolboy)

  As the `DBConnection` documentation states, if using a pool it's necessary to
  pass a `:pool` option with the pool module being used to every call. For
  example:

      {:ok, _pool} = Xandra.start_link(name: :xandra_pool, pool: DBConnection.Poolboy)
      Xandra.execute!(:xandra_pool, "SELECT * FROM users", _params = [], pool: DBConnection.Poolboy)

  ### Using a keyspace for new connections

  It is common to start a Xandra connection or pool of connections that will use
  a single keyspace for their whole life span. Doing something like:

      {:ok, conn} = Xandra.start_link()
      Xandra.execute!(conn, "USE my_keyspace")

  will work just fine when you only have one connection. If you have a pool of
  connections (with the `:pool` option), however, the code above won't work:
  that code would start the pool, and then checkout one connection from the pool
  to execute the `USE my_keyspace` query. That specific connection will then be
  using the `my_keyspace` keyspace, but all other connections in the pool will
  not. Fortunately, `DBConnection` provides an option we can use to solve this
  problem: `:after_connect`. This option can specify a function that will be run
  after each single connection to Cassandra. This function will take a
  connection and can be used to setup that connection; since this function is
  run for every established connection, it will work well with pools as well.

      {:ok, conn} = Xandra.start_link(after_connect: fn(conn) -> Xandra.execute(conn, "USE my_keyspace") end)

  See the documentation for `DBConnection.start_link/2` for more information
  about this option.
  """
  @spec start_link(start_options) :: GenServer.on_start()
  def start_link(options \\ []) when is_list(options) do
    options =
      @default_start_options
      |> Keyword.merge(options)
      |> parse_start_options()
      |> Keyword.put(:prepared_cache, Prepared.Cache.new())

    DBConnection.start_link(Connection, options)
  end

  @doc """
  Streams the results of a simple query or a prepared query with the given `params`.

  This function can be used to stream the results of `query` so as not to load
  them entirely in memory. This function doesn't send any query to Cassandra
  right away: it will only execute queries as necessary when results are
  requested out of the returned stream.

  The returned value is a stream of `Xandra.Page` structs, where each of such
  structs contains at most as many rows as specified by the `:page_size`
  option. Every time an element is requested from the stream, `query` will be
  executed with `params` to get that result.

  In order to get each result from Cassandra, `execute!/4` is used: this means
  that if there is an error (such as a network error) when executing the
  queries, that error will be raised.

  ### Simple or prepared queries

  Regardless of `query` being a simple query or a prepared query, this function
  will execute it every time a result is needed from the returned stream. For
  this reason, it is usually a good idea to use prepared queries when streaming.

  ## Options

  `options` supports all the options supported by `execute/4`, with the same
  default values.

  ## Examples

      prepared = Xandra.prepare!(conn, "SELECT * FROM users")
      users_stream = Xandra.stream_pages!(conn, prepared, _params = [], page_size: 2)

      [%Xandra.Page{} = _page1, %Xandra.Page{} = _page2] = Enum.take(users_stream, 2)

  """
  @spec stream_pages!(conn, statement | Prepared.t(), values, Keyword.t()) :: Enumerable.t()
  def stream_pages!(conn, query, params, options \\ [])

  def stream_pages!(conn, statement, params, options) when is_binary(statement) do
    %PageStream{conn: conn, query: statement, params: params, options: options}
  end

  def stream_pages!(conn, %Prepared{} = prepared, params, options) do
    %PageStream{conn: conn, query: prepared, params: params, options: options}
  end

  @doc """
  Prepares the given query.

  This function prepares the given statement on the Cassandra server. If
  preparation is successful and there are no network errors while talking to the
  server, `{:ok, prepared}` is returned, otherwise `{:error, error}` is
  returned.

  The returned prepared query can be run through `execute/4`, or used inside a
  batch (see `Xandra.Batch`).

  Errors returned by this function can be either `Xandra.Error` or
  `Xandra.ConnectionError` structs. See the module documentation for more
  information about errors.

  Supports all the options supported by `DBConnection.prepare/3`, and the
  following additional options:

    * `:force` - (boolean) when `true`, forces the preparation of the query on
      the server instead of trying to read the prepared query from cache. See
      the "Prepared queries cache" section below. Defaults to `false`.

    * `:compressor` - (module) the compressor module used to compress and
      decompress data. See the "Compression" section in the module
      documentation. By default, this option is not present.

  ## Prepared queries cache

  Since Cassandra prepares queries on a per-node basis (and not on a
  per-connection basis), Xandra internally caches prepared queries for each
  connection or pool of connections. This means that if you prepare a query that
  was already prepared, no action will be executed on the Cassandra server and
  the prepared query will be returned from the cache.

  If the Cassandra node goes down, however, the prepared query will be
  invalidated and trying to use the one from cache will result in a
  `Xandra.Error`. However, this is automatically handled by Xandra: when such an
  error is returned, Xandra will first retry to prepare the query and only
  return an error if the preparation fails.

  If you want to ensure a query is prepared on the server, you can set the
  `:force` option to `true`.

  ## Examples

      {:ok, prepared} = Xandra.prepare(conn, "SELECT * FROM users WHERE id = ?")
      {:ok, _page} = Xandra.execute(conn, prepared, [_id = 1])

      {:error, %Xandra.Error{reason: :invalid_syntax}} = Xandra.prepare(conn, "bad syntax")

      # Force a query to be prepared on the server and not be read from cache:
      Xandra.prepare!(conn, "SELECT * FROM users WHERE ID = ?", force: true)

  """
  @spec prepare(conn, statement, Keyword.t()) :: {:ok, Prepared.t()} | {:error, error}
  def prepare(conn, statement, options \\ []) when is_binary(statement) do
    DBConnection.prepare(conn, %Prepared{statement: statement}, options)
  end

  @doc """
  Prepares the given query, raising if there's an error.

  This function works exactly like `prepare/3`, except it returns the prepared
  query directly if preparation succeeds, otherwise raises the returned error.

  ## Examples

      prepared = Xandra.prepare!(conn, "SELECT * FROM users WHERE id = ?")
      {:ok, _page} = Xandra.execute(conn, prepared, [_id = 1])

  """
  @spec prepare!(conn, statement, Keyword.t()) :: Prepared.t() | no_return
  def prepare!(conn, statement, options \\ []) do
    case prepare(conn, statement, options) do
      {:ok, result} -> result
      {:error, exception} -> raise(exception)
    end
  end

  @doc """
  Executes the given simple query, prepared query, or batch query.

  Returns `{:ok, result}` if executing the query was successful, or `{:error,
  error}` otherwise. The meaning of the `params_or_options` argument depends on
  what `query` is:

    * if `query` is a batch query, than `params_or_options` has to be a list of
      options that will be used to run the batch query (since batch queries
      don't use parameters as parameters are attached to each query in the
      batch).

    * if `query` is a simple query (a string) or a prepared query, then
      `params_or_opts` is a list or map of parameters, and this function is
      exactly the same as calling `execute(conn, query, params_or_options, [])`.

  When `query` is a batch query, successful results will always be `Xandra.Void`
  structs.

  When `{:error, error}` is returned, `error` can be either a `Xandra.Error` or
  a `Xandra.ConnectionError` struct. See the module documentation for more
  information on errors.

  ## Options for batch queries

  When `query` is a batch query, `params_or_options` is a list of options. All
  options supported by `DBConnection.execute/4` are supported, and the following
  additional batch-specific options:

    * `:consistency` - same as the `:consistency` option described in the
      documentation for `execute/4`.

    * `:serial_consistency` - same as the `:serial_consistency` option described
      in the documentation for `execute/4`.

    * `:timestamp` - (integer) using this option means that the provided
      timestamp will apply to all the statements in the batch that do not
      explicitly specify a timestamp.

  ## Examples

  For examples on executing simple queries or prepared queries, see the
  documentation for `execute/4`. Examples below specifically refer to batch
  queries. See the documentation for `Xandra.Batch` for more information about
  batch queries and how to construct them.

      prepared_insert = Xandra.prepare!(conn, "INSERT (email, name) INTO users VALUES (?, ?)")

      batch =
        Xandra.Batch.new()
        |> Xandra.Batch.add(prepared_insert, ["abed@community.com", "Abed Nadir"])
        |> Xandra.Batch.add(prepared_insert, ["troy@community.com", "Troy Barnes"])
        |> Xandra.Batch.add(prepared_insert, ["britta@community.com", "Britta Perry"])

      # Execute the batch:
      Xandra.execute(conn, batch)
      #=> {:ok, %Xandra.Void{}}

      # Execute the batch with a default timestamp for all statements:
      Xandra.execute(conn, batch, timestamp: System.system_time(:millisecond) - 1_000)
      #=> {:ok, %Xandra.Void{}}

  All `DBConnection.execute/4` options are supported here as well:

      Xandra.execute(conn, batch, pool: DBConnection.Poolboy)
      #=> {:ok, %Xandra.Void{}}

  """
  @spec execute(conn, statement | Prepared.t(), values) :: {:ok, result} | {:error, error}
  @spec execute(conn, Batch.t(), Keyword.t()) :: {:ok, Xandra.Void.t()} | {:error, error}
  def execute(conn, query, params_or_options \\ [])

  def execute(conn, statement, params) when is_binary(statement) do
    execute(conn, statement, params, _options = [])
  end

  def execute(conn, %Prepared{} = prepared, params) do
    execute(conn, prepared, params, _options = [])
  end

  def execute(conn, %Batch{} = batch, options) when is_list(options) do
    execute_with_retrying(conn, batch, nil, options)
  end

  @doc """
  Executes the given simple query or prepared query with the given parameters.

  Returns `{:ok, result}` where `result` is the result of executing `query` if
  the execution is successful (there are no network errors or semantic errors
  with the query), or `{:error, error}` otherwise.

  `result` can be one of the following:

    * a `Xandra.Void` struct - returned for queries such as `INSERT`, `UPDATE`,
      or `DELETE`.

    * a `Xandra.SchemaChange` struct - returned for queries that perform changes
      on the schema (such as creating tables).

    * a `Xandra.SetKeyspace` struct - returned for `USE` queries.

    * a `Xandra.Page` struct - returned for queries that return rows (such as
      `SELECT` queries).

  The properties of each of the results listed above are described in each
  result's module.

  ## Options

  This function accepts all options accepted by `DBConnection.execute/4`, plus
  the following ones:

    * `:consistency` - (atom) specifies the consistency level for the given
      query. See the Cassandra documentation for more information on consistency
      levels. The value of this option can be one of:
      * `:one` (default)
      * `:two`
      * `:three`
      * `:any`
      * `:quorum`
      * `:all`
      * `:local_quorum`
      * `:each_quorum`
      * `:serial`
      * `:local_serial`
      * `:local_one`

    * `:page_size` - (integer) the size of a page of results. If `query` returns
      `Xandra.Page` struct, that struct will contain at most `:page_size` rows
      in it. Defaults to `10_000`.

    * `:paging_state` - (binary) the offset where rows should be
      returned from. By default this option is not present and paging starts
      from the beginning. See the "Paging" section below for more information on
      how to page queries.

    * `:timestamp` - (integer) the default timestamp for the query (in
      microseconds). If provided, overrides the server-side assigned timestamp;
      however, a timestamp in the query itself will still override this
      timestamp.

    * `:serial_consistency` - (atom) specifies the serial consistency to use for
      executing the given query. Can be of `:serial` and `:local_serial`.

    * `:compressor` - (module) the compressor module used to compress and
      decompress data. See the "Compression" section in the module
      documentation. By default, this option is not present.

    * `:retry_strategy` - (module) the module implementing the
      `Xandra.RetryStrategy` behaviour that is used in case the query fails to
      determine whether to retry it or not. See the "Retrying failed queries"
      section in the module documentation. By default, this option is not
      present.

    * `:date_format` - (`:date` or `:integer`) controls the format in which
      dates are returned. When set to `:integer` the returned value is
      a number of days from the Unix epoch, a date struct otherwise.
      Defaults to `:date`.

    * `:time_format` - (`:time` or `:integer`) controls the format in which
      times are returned. When set to `:integer` the returned value is
      a number of nanoseconds from midnight, a time struct otherwise.
      Defaults to `:time`.

    * `:timestamp_format` - (`:datetime` or `:integer`) controls the format in which
      timestamps are returned. When set to `:integer` the returned value is
      a number of milliseconds from the Unix epoch, a datetime struct otherwise.
      Defaults to `:datetime`.

  ## Parameters

  The `params` argument specifies parameters to use when executing the query; it
  can be either a list of positional parameters (specified via `?` in the query)
  or a map of named parameters (specified as `:named_parameter` in the
  query). When `query` is a simple query, the value of each parameter must be a
  two-element tuple specifying the type used to encode the value and the value
  itself; when `query` is a prepared query, this is not necessary (and values
  can just be values) as the type information is encoded in the prepared
  query. See the module documenatation for more information about query
  parameters, types, and encoding values.

  ## Examples

  Executing a simple query (which is just a string):

      statement = "INSERT INTO users (first_name, last_name) VALUES (:first_name, :last_name)"
      {:ok, %Xandra.Void{}} = Xandra.execute(conn, statement, %{
        "first_name" => {"text", "Chandler"},
        "last_name" => {"text", "Bing"},
      })

  Executing the query when `atom_keys: true` has been specified in `Xandra.start_link/1`:

      Xandra.execute(conn, statement, %{
        first_name: {"text", "Chandler"},
        last_name: {"text", "Bing"}
      })

  Executing a prepared query:

      prepared = Xandra.prepare!(conn, "INSERT INTO users (first_name, last_name) VALUES (?, ?)")
      {:ok, %Xandra.Void{}} = Xandra.execute(conn, prepared, ["Monica", "Geller"])

  Performing a `SELECT` query and using `Enum.to_list/1` to convert the
  `Xandra.Page` result to a list of rows:

      statement = "SELECT * FROM users"
      {:ok, %Xandra.Page{} = page} = Xandra.execute(conn, statement, _params = [])
      Enum.to_list(page)
      #=> [%{"first_name" => "Chandler", "last_name" => "Bing"},
      #=>  %{"first_name" => "Monica", "last_name" => "Geller"}]

  Performing the query when `atom_keys: true` has been specified in `Xandra.start_link/1`:

      {:ok, page} = Xandra.execute(conn, statement, _params = [])
      Enum.to_list(page)
      #=> [%{first_name:  "Chandler", last_name: "Bing"},
      #=>  %{first_name: "Monica", last_name: "Geller"}]

  Ensuring the write is written to the commit log and memtable of at least three replica nodes:

      statement = "INSERT INTO users (first_name, last_name) VALUES ('Chandler', 'Bing')"
      {:ok, %Xandra.Void{}} = Xandra.execute(conn, statement, _params = [], consistency: :three)

  This function supports all options supported by `DBConnection.execute/4`; for
  example, if the `conn` connection was started with `pool: DBConnection.Poolboy`,
  then the `:pool` option would have to be passed here as well:

      statement = "DELETE FROM users WHERE first_name = 'Chandler'"
      {:ok, %Xandra.Void{}} = Xandra.execute(conn, statement, _params = [], pool: DBConnection.Poolboy)

  ## Paging

  Since `execute/4` supports the `:paging_state` option, it is possible to manually
  implement paging. For example, given the following prepared query:

      prepared = Xandra.prepare!(conn, "SELECT first_name FROM users")

  We can now execute such query with a specific page size using the `:page_size`
  option:

      {:ok, %Xandra.Page{} = page} = Xandra.execute(conn, prepared, [], page_size: 2)

  Since `:page_size` is `2`, `page` will contain at most `2` rows:

      Enum.to_list(page)
      #=> [%{"first_name" => "Ross"}, %{"first_name" => "Rachel"}]

  Now, we can pass `page.paging_state` as the value of the `:paging_state` option to let the paging
  start from where we left off:

      {:ok, %Xandra.Page{} = new_page} = Xandra.execute(conn, prepared, [], page_size: 2, paging_state: page.paging_state)
      Enum.to_list(page)
      #=> [%{"first_name" => "Joey"}, %{"first_name" => "Phoebe"}]

  However, using `:paging_state` and `:page_size` directly with `execute/4` is not
  recommended when the intent is to "stream" a query. For that, it's recommended
  to use `stream_pages!/4`. Also note that if the `:paging_state` option is set to `nil`,
  meaning there are no more pages to fetch, an `ArgumentError` exception will be raised;
  be sure to check for this with `page.paging_state != nil`.
  """
  @spec execute(conn, statement | Prepared.t(), values, Keyword.t()) ::
          {:ok, result} | {:error, error}
  def execute(conn, query, params, options)

  def execute(conn, statement, params, options) when is_binary(statement) do
    query = %Simple{statement: statement}
    execute_with_retrying(conn, query, params, validate_paging_state(options))
  end

  def execute(conn, %Prepared{} = prepared, params, options) do
    execute_with_retrying(conn, prepared, params, validate_paging_state(options))
  end

  @doc """
  Executes the given simple query, prepared query, or batch query, raising if
  there's an error.

  This function behaves exactly like `execute/3`, except that it returns
  successful results directly and raises on errors.

  ## Examples

      Xandra.execute!(conn, "INSERT INTO users (name, age) VALUES ('Jane', 29)")
      #=> %Xandra.Void{}

  """
  @spec execute!(conn, statement | Prepared.t(), values) :: result | no_return
  @spec execute!(conn, Batch.t(), Keyword.t()) :: Xandra.Void.t() | no_return
  def execute!(conn, query, params_or_options \\ []) do
    case execute(conn, query, params_or_options) do
      {:ok, result} -> result
      {:error, exception} -> raise(exception)
    end
  end

  @doc """
  Executes the given simple query, prepared query, or batch query, raising if
  there's an error.

  This function behaves exactly like `execute/4`, except that it returns
  successful results directly and raises on errors.

  ## Examples

      statement = "INSERT INTO users (name, age) VALUES ('John', 43)"
      Xandra.execute!(conn, statement, _params = [], consistency: :quorum)
      #=> %Xandra.Void{}

  """
  @spec execute!(conn, statement | Prepared.t(), values, Keyword.t()) :: result | no_return
  def execute!(conn, query, params, options) do
    case execute(conn, query, params, options) do
      {:ok, result} -> result
      {:error, exception} -> raise(exception)
    end
  end

  @doc """
  Acquires a locked connection from `conn` and executes `fun` passing such
  connection as the argument.

  All options are forwarded to `DBConnection.run/3` (and thus some of them to
  the underlying pool).

  The return value of this function is the return value of `fun`.

  ## Examples

  Preparing a query and executing it on the same connection:

      Xandra.run(conn, fn conn ->
        prepared = Xandra.prepare!(conn, "INSERT INTO users (name, age) VALUES (:name, :age)")
        Xandra.execute!(conn, prepared, %{"name" => "John", "age" => 84})
      end)

  """
  @spec run(conn, Keyword.t(), (conn -> result)) :: result when result: var
  def run(conn, options \\ [], fun) when is_function(fun, 1) do
    DBConnection.run(conn, fun, options)
  end

  defp reprepare_queries(conn, [%Simple{} | rest], options) do
    reprepare_queries(conn, rest, options)
  end

  defp reprepare_queries(conn, [%Prepared{statement: statement} | rest], options) do
    with {:ok, _prepared} <- prepare(conn, statement, Keyword.put(options, :force, true)) do
      reprepare_queries(conn, rest, options)
    end
  end

  defp reprepare_queries(_conn, [], _options) do
    :ok
  end

  defp validate_paging_state(options) do
    case Keyword.fetch(options, :paging_state) do
      {:ok, nil} ->
        raise ArgumentError, "no more pages are available"

      {:ok, value} when not is_binary(value) ->
        raise ArgumentError,
              "expected a binary as the value of the :paging_state option, " <>
                "got: #{inspect(value)}"

      _other ->
        maybe_put_paging_state(options)
    end
  end

  defp maybe_put_paging_state(options) do
    case Keyword.pop(options, :cursor) do
      {%Page{paging_state: nil}, _options} ->
        raise ArgumentError, "no more pages are available"

      {%Page{paging_state: paging_state}, options} ->
        IO.warn("the :cursor option is deprecated, please use :paging_state instead")
        Keyword.put(options, :paging_state, paging_state)

      {nil, options} ->
        options

      {other, _options} ->
        raise ArgumentError,
              "expected a Xandra.Page struct as the value of the :cursor option, " <>
                "got: #{inspect(other)}"
    end
  end

  defp execute_with_retrying(conn, query, params, options) do
    case Keyword.pop(options, :retry_strategy) do
      {nil, options} ->
        execute_without_retrying(conn, query, params, options)

      {retry_strategy, options} ->
        execute_with_retrying(conn, query, params, options, retry_strategy)
    end
  end

  defp execute_with_retrying(conn, query, params, options, retry_strategy) do
    with {:error, reason} <- execute_without_retrying(conn, query, params, options) do
      {retry_state, options} =
        Keyword.pop_lazy(options, :retrying_state, fn ->
          retry_strategy.new(options)
        end)

      case retry_strategy.retry(reason, options, retry_state) do
        :error ->
          {:error, reason}

        {:retry, new_options, new_retry_state} ->
          new_options = Keyword.put(new_options, :retrying_state, new_retry_state)
          execute_with_retrying(conn, query, params, new_options, retry_strategy)

        other ->
          raise ArgumentError,
                "invalid return value #{inspect(other)} from " <>
                  "retry strategy #{inspect(retry_strategy)} " <>
                  "with state #{inspect(retry_state)}"
      end
    end
  end

  defp execute_without_retrying(conn, %Batch{} = batch, nil, options) do
    run(conn, options, fn conn ->
      case DBConnection.execute(conn, batch, nil, options) do
        {:ok, %Error{reason: :unprepared}} ->
          with :ok <- reprepare_queries(conn, batch.queries, options) do
            execute(conn, batch, options)
          end

        {:ok, %Error{} = error} ->
          {:error, error}

        other ->
          other
      end
    end)
  end

  defp execute_without_retrying(conn, %Simple{} = query, params, options) do
    with {:ok, %Error{} = error} <- DBConnection.execute(conn, query, params, options) do
      {:error, error}
    end
  end

  defp execute_without_retrying(conn, %Prepared{} = prepared, params, options) do
    run(conn, options, fn conn ->
      case DBConnection.execute(conn, prepared, params, options) do
        {:ok, %Error{reason: :unprepared}} ->
          # We can ignore the newly returned prepared query since it will have the
          # same id of the query we are repreparing.
          case DBConnection.prepare_execute(
                 conn,
                 prepared,
                 params,
                 Keyword.put(options, :force, true)
               ) do
            {:ok, _prepared, %Error{} = error} ->
              {:error, error}

            {:ok, _prepared, result} ->
              {:ok, result}

            {:error, _reason} = error ->
              error
          end

        {:ok, %Error{} = error} ->
          {:error, error}

        other ->
          other
      end
    end)
  end

  defp parse_start_options(options) do
    cluster? = options[:pool] == Xandra.Cluster

    Enum.flat_map(options, fn
      {:nodes, nodes} when cluster? ->
        [nodes: Enum.map(nodes, &parse_node/1)]

      {:nodes, [string]} ->
        {address, port} = parse_node(string)
        [address: address, port: port]

      {:nodes, _nodes} ->
        raise ArgumentError,
              "multi-node use requires the :pool option to be set to Xandra.Cluster"

      {_key, _value} = option ->
        [option]
    end)
  end

  defp parse_node(string) do
    case String.split(string, ":", parts: 2) do
      [address, port] ->
        case Integer.parse(port) do
          {port, ""} ->
            {String.to_charlist(address), port}

          _ ->
            raise ArgumentError, "invalid item #{inspect(string)} in the :nodes option"
        end

      [address] ->
        {String.to_charlist(address), @default_port}
    end
  end
end
