defmodule Xandra.Cluster do
  @moduledoc """
  Connection to a Cassandra cluster.

  This module is a "proxy" connection with support for connecting to multiple
  nodes in a Cassandra cluster and executing queries on such nodes based on a
  given *policy*.

  ## Usage

  This module manages pools of connections to different nodes in a Cassandra cluster.
  Each pool is a pool of `Xandra` connections to a specific node.

  The API provided by this module mirrors the API provided by the `Xandra`
  module. Queries executed through this module will be "routed" to nodes
  in the provided list of nodes based on a policy. See the
  ["Load balancing policies" section](#module-load-balancing-policies).

  Regardless of the underlying pool, `Xandra.Cluster` will establish
  one extra connection to a node in the cluster for internal purposes.
  We refer to this connection as the **control connection**.

  Here is an example of how one could use `Xandra.Cluster` to connect to a cluster:

      Xandra.Cluster.start_link(
        nodes: ["cassandra1.example.net", "cassandra2.example.net"],
        pool_size: 10,
      )

  The code above will establish a pool of ten connections to each of the nodes
  specified in `:nodes`, plus one extra connection used for internal
  purposes, for a total of twenty-one connections going out of the machine.

  ## Child Specification

  `Xandra.Cluster` implements a `child_spec/1` function, so it can be used as a child
  under a supervisor:

      children = [
        # ...,
        {Xandra.Cluster, nodes: ["cassandra-seed.example.net"]}
      ]

  ## Contact Points and Cluster Discovery

  `Xandra.Cluster` auto-discovers peer nodes in the cluster, by using the `system.peers`
  built-in Cassandra table. Once Xandra discovers peers, it opens a pool of connections
  to a subset of the peers based on the chosen load-balancing policy (see below).

  The `:nodes` option in `start_link/1` specifies the **contact points**. The contact
  points are used to discover the rest of the nodes in the cluster. It's generally
  a good idea to provide multiple contacts points, so that if some of those are
  unreachable, the others can be used to discover the rest of the cluster. `Xandra.Cluster`
  tries to connect to contact points in the order they are specified in the `:nodes`
  option, initially ignoring the chosen load-balancing policy. Once a connection is
  established, then that contact point is used to discover the rest of the cluster
  and open connection pools according to the load-balancing policy.

  Xandra also **refreshes** the cluster topology periodically. See the
  `:refresh_topology_interval` option in `start_link/1`.

  ## Load-balancing Policies

  `Xandra.Cluster` uses customizable "load-balancing policies" to manage nodes
  in the cluster. A load-balancing policy is a module that implements the
  `Xandra.Cluster.LoadBalancing` behaviour. Xandra uses load-balancing policies
  for these purposes:

    * Choosing which node to execute a query on
    * Choosing which nodes to open pools of connections to (see the `:target_pools`
      option in `start_link/1`)
    * Choosing which node the control connection connects to (or **re-connects** to
      in case of disconnections)

  Xandra ships with the following built-in load-balancing policies:

    * `Xandra.Cluster.LoadBalancingPolicy.Random` - it will choose one of the
      connected nodes at random and execute the query on that node.
    * `Xandra.Cluster.LoadBalancingPolicy.DCAwareRoundRobin` - it will execute the
      queries on the nodes in a round robin manner, prioritizing the current DC.

  ## Disconnections and Reconnections

  `Xandra.Cluster` also supports nodes disconnecting and reconnecting: if Xandra
  detects one of the nodes in `:nodes` going down, it will not execute queries
  against it anymore, but will start executing queries on it as soon as it
  detects such node is back up.

  If all specified nodes happen to be down when a query is executed, a
  `Xandra.ConnectionError` with reason `{:cluster, :not_connected}` will be
  returned.

  ## Telemetry

  `Xandra.Cluster` emits several Telemetry events to help you log, instrument,
  and debug your application. See the [*Telemetry Events*](telemetry-events.html)
  page in the guides for a comprehensive list of the events that Xandra emits.
  """

  alias Xandra.{Batch, ConnectionError, Prepared, RetryStrategy}
  alias Xandra.Cluster.{ControlConnection, Host, Pool}

  @typedoc """
  A Xandra cluster.
  """
  @type cluster :: GenServer.server()

  @default_port 9042

  @start_link_opts_schema [
    nodes: [
      type: {:list, {:custom, Xandra.OptionsValidators, :validate_node, []}},
      default: ["127.0.0.1"],
      type_doc: "list of `t:String.t/0`",
      doc: """
      A list of nodes to use as *contact points* when setting up the cluster. Each node in this
      list must be a hostname (`"cassandra.example.net"`), IPv4 (`"192.168.0.100"`),
      or IPv6 (`"16:64:c8:0:2c:58:5c:c7"`) address. An optional port can be specified by
      including `:<port>` after the address, such as `"cassandra.example.net:9876"`.
      See the [*Contact points and cluster discovery*
      section](#module-contact-points-and-cluster-discovery) in the module documentation.
      """
    ],
    load_balancing: [
      type: {:or, [{:in, [:random]}, :mod_arg]},
      type_doc: "`{module(), term()}` or `:random`",
      default: :random,
      doc: """
      Load balancing "policy". See the [*Load balancing policies*
      section](#module-load-balancing-policies) in the module documentation.
      The policy must be expressed as a `{module, options}` tuple, where `module`
      is a module that implements the `Xandra.Cluster.LoadBalancingPolicy` behaviour, and
      `options` is any term that is passed to the `c:Xandra.Cluster.LoadBalancingPolicy.init/1`
      callback. This option changed in v0.15.0.
      Before *v0.15.0*, the only supported values were `:priority` and `:random`.
      `:random` is deprecated in favor of using `{Xandra.Cluster.LoadBalancingPolicy.Random, []}`.
      `:priority` has been removed.
      """
    ],
    autodiscovery: [
      type: :boolean,
      doc: """
      (**deprecated**) Whether to enable autodiscovery. Since v0.15.0, this option is deprecated
      and autodiscovery is always enabled.
      """,
      deprecated: """
      :autodiscovery is deprecated since v0.15.0 and now always enabled due to internal changes
      to Xandra.Cluster.
      """
    ],
    autodiscovered_nodes_port: [
      type: {:in, 0..65535},
      default: @default_port,
      type_doc: "`t::inet.port_number/0`",
      doc: """
      The port to use when connecting to autodiscovered nodes. Cassandra does not advertise
      the port of nodes when discovering them, so you'll need to specify one explicitly.
      This might get fixed in future Cassandra versions.
      """
    ],
    refresh_topology_interval: [
      type: :timeout,
      default: 300_000,
      doc: """
      The interval at which Xandra will refresh the cluster topology by querying the control
      connection to discover peers. When the connection refreshes the topology, it will
      also start and stop pools for new and removed nodes, effectively "syncing" with
      the cluster. *Available since v0.15.0*.
      """
    ],
    target_pools: [
      type: :pos_integer,
      default: 2,
      doc: """
      The number of nodes to start pools to. Each pool will use the `:pool_size` option to
      determine how many single connections to open to that node. This number is a *target*
      number, which means that sometimes there might not be enough nodes to start this many
      pools. Xandra won't ever start more than `:target_pools` pools. *Available since v0.15.0*.
      """
    ],
    name: [
      type: :any,
      doc: """
      The name to register this cluster under. Follows the name registration rules of `GenServer`.
      """
    ],
    sync_connect: [
      type: {:or, [:timeout, {:in, [false]}]},
      default: false,
      doc: """
      Whether to wait for at least one connection to a node in the cluster to be established
      before returning from `start_link/1`. If `false`, connecting is async, which means that
      even if `start_link/1` returns `{:ok, pid}`, that's the PID of the cluster process,
      which has not necessarily established any connections yet. If this option is
      an integer or `:infinity` (that is, a term of type `t:timeout/0`), then this function
      only returns when at least one node connection is established. If the timeout expires,
      this function returns `{:error, :sync_connect_timeout}`. *Available since v0.16.0*.

      This is only useful in rare cases when you want to make sure that the has connected
      at least once before returning from `start_link/1`. This is fragile though, because
      the cluster could connect once and then drop connections right away, so this doesn't
      mean that the cluster is connected, but rather that it *connected at least once*.
      This is useful, for example, in test suites where you're not worried about
      resiliency but rather race conditions. In most cases, the
      `:queue_checkouts_before_connecting` option is what you want.
      """
    ],
    queue_checkouts_before_connecting: [
      type: :keyword_list,
      doc: """
      Controls how to handle checkouts that go through the cluster *before* the cluster
      is able to establish a connection to **any node**. Whenever you run a cluster function,
      the cluster checks out a connection from one of the connected nodes and executes the
      request on that connection. However, if you try to run any cluster function before the
      cluster connects to any of the nodes, you'll likely get `Xandra.ConnectionError`s
      with reason `{:cluster, :not_connected}`. This is because the cluster needs to establish
      at least one connection to one node before it can execute requests. This option addresses
      this issue by queueing "checkout requests" until the cluster establishes a connection
      to a node. Once the connection is established, the cluster starts to hand over
      connections. If you want to **disable this behavior**, set `:max_size` to `0`. *Available
      since v0.18.0*. This option supports the following sub-options:
      """,
      keys: [
        max_size: [
          type: :non_neg_integer,
          default: 100,
          doc: """
          The number of checkouts to queue in the cluster and flush as soon as a connection
          is established.
          """
        ],
        timeout: [
          type: :timeout,
          default: 5_000,
          doc: """
          How long to hold on to checkout requests for. When this timeout expires, all requests
          are dropped and a connection error is returned to each caller.
          """
        ]
      ],
      default: []
    ],
    pool_size: [
      type: :pos_integer,
      default: 1,
      doc: """
      The number of connections to open to each node in the cluster. *Available since v0.18.0*.
      """
    ],
    debug: [
      type: :any,
      doc: """
      Same as the `:debug` option in `GenServer.start_link/3`. *Available since v0.18.0*.
      """
    ],
    spawn_opt: [
      type: :any,
      doc: """
      Same as the `:spawn_opt` option in `GenServer.start_link/3`. *Available since v0.18.0*.
      """
    ],
    hibernate_after: [
      type: :any,
      doc: """
      Same as the `:hibernate_after` option in `GenServer.start_link/3`.
      *Available since v0.18.0*.
      """
    ],

    # Internal for testing, not exposed.
    xandra_module: [type: :atom, default: Xandra.Cluster.ConnectionPool, doc: false],
    control_connection_module: [type: :atom, default: ControlConnection, doc: false],
    test_discovered_hosts: [type: :any, default: [], doc: false]
  ]

  @start_link_opts_schema_keys Keyword.keys(@start_link_opts_schema)

  @typedoc """
  Cluster-specific options for `start_link/1`.

  Some of these options are internal and not part of the public API. Only use
  the options explicitly documented in `start_link/1`.
  """
  @typedoc since: "0.15.0"
  @type start_option() :: unquote(NimbleOptions.option_typespec(@start_link_opts_schema))

  @doc """
  Starts connections to a cluster.

  ## Options

  This function accepts all options accepted by `Xandra.start_link/1` and
  and forwards them to each underlying connection or pool of connections. The following
  options are specific to this function:

  #{NimbleOptions.docs(@start_link_opts_schema)}

  > #### Control connection {: .neutral}
  >
  > A `Xandra.Cluster` starts **one additional "control connection"** to one of the
  > nodes in the cluster. This could be a node in the given `:nodes` (a *contact point*)
  > or a discovered peer in the cluster. See the [*Contact points and cluster discovery*
  > section](#module-contact-points-and-cluster-discovery) in the module documentation.

  ## Examples

  Starting a Xandra cluster using two nodes as the contact points:

      {:ok, cluster} =
        Xandra.Cluster.start_link(nodes: ["cassandra1.example.net", "cassandra2.example.net"])

  Starting a pool of five connections to each node in the same cluster as the given
  contact point:

      {:ok, cluster} =
        Xandra.Cluster.start_link(nodes: ["cassandra-seed.example.net"], pool_size: 5)

  Passing options down to each connection:

      {:ok, cluster} =
        Xandra.Cluster.start_link(
          nodes: ["cassandra.example.net"],
          keyspace: "my_keyspace"
        )

  """
  @spec start_link([option]) :: GenServer.on_start()
        when option: Xandra.start_option() | start_option()
  def start_link(options) when is_list(options) do
    {cluster_opts, connection_opts} = Keyword.split(options, @start_link_opts_schema_keys)
    cluster_opts = NimbleOptions.validate!(cluster_opts, @start_link_opts_schema)
    connection_opts = NimbleOptions.validate!(connection_opts, Xandra.start_link_opts_schema())
    Pool.start_link(cluster_opts, connection_opts)
  end

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(options) do
    id = Keyword.get(options, :name, __MODULE__)
    %{id: id, start: {__MODULE__, :start_link, [options]}, type: :worker}
  end

  @doc """
  Returns a stream of pages.

  When streaming pages through a cluster, the streaming is done
  from a single node, that is, this function just calls out to
  `Xandra.stream_pages!/4` after choosing a node appropriately.

  All options are forwarded to `Xandra.stream_pages!/4`, including
  retrying options.
  """
  @spec stream_pages!(
          cluster,
          Xandra.statement() | Xandra.Prepared.t(),
          Xandra.values(),
          keyword
        ) ::
          Enumerable.t()
  def stream_pages!(cluster, query, params, options \\ []) do
    with_conn(cluster, &Xandra.stream_pages!(&1, query, params, options))
  end

  @doc """
  Same as `Xandra.prepare/3`.

  Preparing a query through `Xandra.Cluster` will prepare it only on one node,
  according to the load-balancing policy chosen in `start_link/1`. To prepare
  and execute a query on the same node, you could use `run/3`:

      Xandra.Cluster.run(cluster, fn conn ->
        # "conn" is the pool of connections for a specific node.
        prepared = Xandra.prepare!(conn, "SELECT * FROM system.local")
        Xandra.execute!(conn, prepared, _params = [])
      end)

  Thanks to the prepared query cache, we can always reprepare the query and execute
  it because after the first time (on each node) the prepared query will be fetched
  from the cache. However, if a prepared query is unknown on a node, Xandra will
  prepare it on that node on the fly, so we can simply do this as well:

      prepared = Xandra.Cluster.prepare!(cluster, "SELECT * FROM system.local")
      Xandra.Cluster.execute!(cluster, prepared, _params = [])

  Note that this goes through the cluster twice, so there's a high chance that
  the query will be prepared on one node and then executed on another node.
  This is however useful if you want to use the `:retry_strategy` option in
  `execute!/4`: in the `run/3` example above, if you use `:retry_strategy` with
  `Xandra.execute!/3`, the query will be retried on the same pool of connections
  to the same node. `execute!/4` will retry queries going through the cluster
  again instead.
  """
  @spec prepare(cluster, Xandra.statement(), keyword) ::
          {:ok, Xandra.Prepared.t()} | {:error, Xandra.error()}
  def prepare(cluster, statement, options \\ []) when is_binary(statement) do
    with_conn(cluster, &Xandra.prepare(&1, statement, options))
  end

  @doc """
  Same as `prepare/3` but raises in case of errors.

  If the function is successful, the prepared query is returned directly
  instead of in an `{:ok, prepared}` tuple like in `prepare/3`.
  """
  @spec prepare!(cluster, Xandra.statement(), keyword) :: Xandra.Prepared.t()
  def prepare!(cluster, statement, options \\ []) do
    case prepare(cluster, statement, options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Same as `execute/4` but with optional arguments.
  """
  @spec execute(cluster, Xandra.statement() | Xandra.Prepared.t(), Xandra.values()) ::
          {:ok, Xandra.result()} | {:error, Xandra.error()}
  @spec execute(cluster, Xandra.Batch.t(), keyword) ::
          {:ok, Xandra.Void.t()} | {:error, Xandra.error()}
  def execute(cluster, query, params_or_options \\ [])

  def execute(cluster, statement, params) when is_binary(statement) do
    execute(cluster, statement, params, _options = [])
  end

  def execute(cluster, %Prepared{} = prepared, params) do
    execute(cluster, prepared, params, _options = [])
  end

  def execute(cluster, %Batch{} = batch, options) when is_list(options) do
    options_without_retry_strategy = Keyword.delete(options, :retry_strategy)

    with_conn_and_retrying(
      cluster,
      options,
      fn conn ->
        Xandra.execute(conn, batch, options_without_retry_strategy)
      end
    )
  end

  @doc """
  Executes a query on a node in the cluster.

  This function executes a query on a node in the cluster. The node is chosen based
  on the load-balancing policy given in `start_link/1`.

  Supports the same options as `Xandra.execute/4`. In particular, the `:retry_strategy`
  option is cluster-aware, meaning that queries are retried on possibly different nodes
  in the cluster.
  """
  @spec execute(cluster, Xandra.statement() | Xandra.Prepared.t(), Xandra.values(), keyword) ::
          {:ok, Xandra.result()} | {:error, Xandra.error()}
  def execute(cluster, query, params, options) do
    options_without_retry_strategy = Keyword.delete(options, :retry_strategy)

    with_conn_and_retrying(
      cluster,
      options,
      fn conn ->
        Xandra.execute(conn, query, params, options_without_retry_strategy)
      end
    )
  end

  @doc """
  Same as `execute/3` but returns the result directly or raises in case of errors.
  """
  @spec execute!(cluster, Xandra.statement() | Xandra.Prepared.t(), Xandra.values()) ::
          Xandra.result()
  @spec execute!(cluster, Xandra.Batch.t(), keyword) :: Xandra.Void.t()
  def execute!(cluster, query, params_or_options \\ []) do
    case execute(cluster, query, params_or_options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Same as `execute/4` but returns the result directly or raises in case of errors.
  """
  @spec execute(cluster, Xandra.statement() | Xandra.Prepared.t(), Xandra.values(), keyword) ::
          Xandra.result()
  def execute!(cluster, query, params, options) do
    case execute(cluster, query, params, options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Runs a function with a given connection.

  The connection that is passed to `fun` is a Xandra connection, not a
  cluster. This means that you should call `Xandra` functions on it.
  Since the connection is a single connection, it means that it's a connection
  to a specific node, so you can do things like prepare a query and then execute
  it because you can be sure it's prepared on the same node where you're
  executing it.

  ## Examples

      query = "SELECT * FROM system_schema.keyspaces"

      Xandra.Cluster.run(cluster, fn conn ->
        prepared = Xandra.prepare!(conn, query)
        Xandra.execute!(conn, prepared, _params = [])
      end)

  """
  @spec run(cluster, keyword, (Xandra.conn() -> result)) :: result when result: var
  def run(cluster, _options \\ [], fun) do
    with_conn(cluster, fun)
  end

  @doc """
  Synchronously stops the given cluster with the given reason.

  Waits `timeout` milliseconds for the cluster to stop before aborting and exiting.
  """
  @doc since: "0.15.0"
  @spec stop(cluster, term, timeout) :: :ok
  def stop(cluster, reason \\ :normal, timeout \\ :infinity)
      when timeout == :infinity or (is_integer(timeout) and timeout >= 0) do
    Pool.stop(cluster, reason, timeout)
  end

  @doc """
  Returns a list of hosts that the cluster has outgoing connections to.
  """
  @doc since: "0.18.0"
  @spec connected_hosts(cluster) :: [Host.t()]
  def connected_hosts(cluster) do
    Pool.connected_hosts(cluster)
  end

  defp with_conn_and_retrying(cluster, options, fun) when is_function(fun, 1) do
    case Pool.checkout(cluster) do
      {:error, :empty} ->
        action = "checkout from cluster #{inspect(cluster)}"
        {:error, ConnectionError.new(action, {:cluster, :not_connected})}

      {:ok, connected_hosts} ->
        RetryStrategy.run_on_cluster(options, connected_hosts, fun)
    end
  end

  defp with_conn(cluster, fun) do
    case Pool.checkout(cluster) do
      {:ok, [{pool, _host} | _connected_hosts]} ->
        fun.(pool)

      {:error, :empty} ->
        action = "checkout from cluster #{inspect(cluster)}"
        {:error, ConnectionError.new(action, {:cluster, :not_connected})}
    end
  end
end
