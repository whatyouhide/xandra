defmodule Xandra.Cluster do
  @moduledoc """
  Connection to a Cassandra cluster.

  This module is a "proxy" connection with support for connecting to multiple
  nodes in a Cassandra cluster and executing queries on such nodes based on a
  given *strategy*.

  ## Usage

  This module manages connections to different nodes in a Cassandra cluster.
  Each connection to a node is a pool of `Xandra` connections. When a `Xandra.Cluster`
  connection is started, one `Xandra` pool of connections will be started for
  each node specified in the `:nodes` option plus for autodiscovered nodes
  if the `:autodiscovery` option is `true`.

  The API provided by this module mirrors the API provided by the `Xandra`
  module. Queries executed through this module will be "routed" to nodes
  in the provided list of nodes based on a strategy. See the
  ["Load balancing strategies" section](#module-load-balancing-strategies).

  Regardless of the underlying pool, `Xandra.Cluster` will establish
  one extra connection to each node in the specified list of `:nodes` (used for
  internal purposes). See `start_link/1`.

  Here is an example of how one could use `Xandra.Cluster` to connect to
  multiple nodes:

      Xandra.Cluster.start_link(
        nodes: ["cassandra1.example.net", "cassandra2.example.net"],
        pool_size: 10,
      )

  The code above will establish a pool of ten connections to each of the nodes
  specified in `:nodes`, plus two extra connections (one per node) used for internal
  purposes, for a total of twenty-two connections going out of the machine.

  ## Child specification

  `Xandra.Cluster` implements a `child_spec/1` function, so it can be used as a child
  under a supervisor:

      children = [
        # ...,
        {Xandra.Cluster, autodiscovery: true, nodes: ["cassandra-seed.example.net"]}
      ]

  ## Autodiscovery

  When the `:autodiscovery` option is `true`, `Xandra.Cluster` discovers peer
  nodes that live in the same cluster as the nodes specified in the `:nodes`
  option. The nodes in `:nodes` act as **seed nodes**. When nodes in the cluster
  are discovered, a `Xandra` pool of connections is started for each node that
  is in the **same datacenter** as one of the nodes in `:nodes`. For now, there
  is no limit on how many nodes in the same datacenter `Xandra.Cluster`
  discovers and connects to.

  ## Load-balancing strategies

  These are the available load-balancing strategies:

    * `:random` - it will choose one of the connected nodes at random and
      execute the query on that node.

    * `:priority` - it will choose a node to execute the query according
      to the order nodes appear in `:nodes`. Not supported when `:autodiscovery`
      is `true`.

  ## Disconnections and reconnections

  `Xandra.Cluster` also supports nodes disconnecting and reconnecting: if Xandra
  detects one of the nodes in `:nodes` going down, it will not execute queries
  against it anymore, but will start executing queries on it as soon as it
  detects such node is back up.

  If all specified nodes happen to be down when a query is executed, a
  `Xandra.ConnectionError` with reason `{:cluster, :not_connected}` will be
  returned.
  """

  use GenServer

  alias Xandra.Cluster.{ControlConnection, StatusChange, TopologyChange}
  alias Xandra.{Batch, ConnectionError, Prepared, RetryStrategy}

  require Logger
  require Record

  @type cluster :: GenServer.server()

  @default_port 9042

  # State.
  defstruct [
    # Options for the underlying connection pools.
    :pool_options,

    # Load balancing strategy.
    :load_balancing,

    # When autodiscovering nodes, you cannot get their port from C*.
    # Other drivers solve this by providing a static port that the driver
    # uses to connect to any autodiscovered node.
    :autodiscovered_nodes_port,

    # A supervisor that supervises pools.
    :pool_supervisor,

    # The PID of the control connection.
    :control_connection,

    # A map of peername to pool PID pairs.
    pools: %{},

    # Modules to swap processes when testing.
    xandra_mod: nil,
    control_conn_mod: nil
  ]

  start_link_opts_schema = [
    load_balancing: [
      type: {:in, [:priority, :random]},
      default: :random,
      doc: """
      Load balancing "strategy". Either `:random` or `:priority`. See the "Load balancing
      strategies" section in the module documentation. If `:autodiscovery` is `true`,
      the only supported strategy is `:random`.
      """
    ],
    nodes: [
      type: {:list, {:custom, Xandra.OptionsValidators, :validate_node, []}},
      default: ["127.0.0.1"],
      doc: """
      A list of nodes to use as seed nodes when setting up the cluster. Each node in this list
      must be a hostname (`"cassandra.example.net"`), IPv4 (`"192.168.0.100"`),
      or IPv6 (`"16:64:c8:0:2c:58:5c:c7"`) address. An optional port can be specified by
      including `:<port>` after the address, such as `"cassandra.example.net:9876"`.
      The behavior of this option depends on the `:autodiscovery` option. See the "Autodiscovery"
      section. If the `:autodiscovery` option is `false`, the cluster only connects
      to the nodes in `:nodes` and sets up one additional control connection
      for each one of these nodes.
      """
    ],
    autodiscovery: [
      type: :boolean,
      deprecated: """
      :autodiscovery is deprecated since 0.15.0 and now always enabled due to internal changes
      to Xandra.Cluster.
      """
    ],
    autodiscovered_nodes_port: [
      type: {:in, 0..65535},
      default: @default_port,
      doc: """
      The port to use when connecting to autodiscovered nodes. Cassandra does not advertise
      the port of nodes when discovering them, so you'll need to specify one explicitly.
      This might get fixed in future Cassandra versions.
      """
    ],
    name: [
      type: :any,
      doc: """
      The name to register this cluster under. Follows the name registration rules of `GenServer`.
      """
    ],

    # Internal for testing, not exposed.
    xandra_module: [type: :atom, default: Xandra, doc: false],
    control_connection_module: [type: :atom, default: ControlConnection, doc: false]
  ]

  @start_link_opts_schema NimbleOptions.new!(start_link_opts_schema)
  @start_link_opts_schema_keys Keyword.keys(start_link_opts_schema)

  @doc """
  Starts connections to a cluster.

  ## Options

  This function accepts all options accepted by `Xandra.start_link/1` and
  and forwards them to each underlying connection or pool of connections. The following
  options are specific to this function:

  #{NimbleOptions.docs(@start_link_opts_schema)}

  > #### Control connections {: .neutral}
  >
  > A `Xandra.Cluster` starts **one additional "control connection"** for each node.
  >
  > If the `:autodiscovery` option is `false`, then this means one additional connection
  > to each node listed in the `:nodes` option. If `:autodiscovery` is `true`, then
  > this means an additional connection to each node in `:nodes` plus one for each
  > "autodiscovered" node.

  ## Examples

  Starting a cluster connection to two specific nodes in the cluster:

      {:ok, cluster} =
        Xandra.Cluster.start_link(nodes: ["cassandra1.example.net", "cassandra2.example.net"])

  Starting a pool of five connections to nodes in the same cluster as the given
  *seed node*:

      {:ok, cluster} =
        Xandra.Cluster.start_link(nodes: ["cassandra-seed.example.net"], pool_size: 5)

  Passing options down to each connection:

      {:ok, cluster} =
        Xandra.Cluster.start_link(
          nodes: ["cassandra.example.net"],
          after_connect: &Xandra.execute!(&1, "USE my_keyspace")
        )

  """
  @spec start_link([option]) :: GenServer.on_start()
        when option: Xandra.start_option() | {atom(), term()}
  def start_link(options) when is_list(options) do
    {cluster_opts, pool_opts} = Keyword.split(options, @start_link_opts_schema_keys)
    cluster_opts = NimbleOptions.validate!(cluster_opts, @start_link_opts_schema)

    {nodes, cluster_opts} = Keyword.pop!(cluster_opts, :nodes)

    state = %__MODULE__{
      pool_options: pool_opts,
      load_balancing: Keyword.fetch!(cluster_opts, :load_balancing),
      autodiscovered_nodes_port: Keyword.fetch!(cluster_opts, :autodiscovered_nodes_port),
      xandra_mod: Keyword.fetch!(cluster_opts, :xandra_module),
      control_conn_mod: Keyword.fetch!(cluster_opts, :control_connection_module)
    }

    GenServer.start_link(__MODULE__, {state, nodes}, Keyword.take(cluster_opts, [:name]))
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
  according to the load balancing strategy chosen in `start_link/1`. To prepare
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
  @spec prepare!(cluster, Xandra.statement(), keyword) :: Xandra.Prepared.t() | no_return
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
    with_conn_and_retrying(cluster, options, &Xandra.execute(&1, batch, options))
  end

  @doc """
  Executes a query on a node in the cluster.

  This function executes a query on a node in the cluster. The node is chosen based
  on the load balancing strategy given in `start_link/1`.

  Supports the same options as `Xandra.execute/4`. In particular, the `:retry_strategy`
  option is cluster-aware, meaning that queries are retried on possibly different nodes
  in the cluster.
  """
  @spec execute(cluster, Xandra.statement() | Xandra.Prepared.t(), Xandra.values(), keyword) ::
          {:ok, Xandra.result()} | {:error, Xandra.error()}
  def execute(cluster, query, params, options) do
    with_conn_and_retrying(cluster, options, &Xandra.execute(&1, query, params, options))
  end

  @doc """
  Same as `execute/3` but returns the result directly or raises in case of errors.
  """
  @spec execute!(cluster, Xandra.statement() | Xandra.Prepared.t(), Xandra.values()) ::
          Xandra.result() | no_return
  @spec execute!(cluster, Xandra.Batch.t(), keyword) ::
          Xandra.Void.t() | no_return
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
          Xandra.result() | no_return
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
  def run(cluster, options \\ [], fun) do
    with_conn(cluster, &Xandra.run(&1, options, fun))
  end

  defp with_conn_and_retrying(cluster, options, fun) do
    RetryStrategy.run_with_retrying(options, fn -> with_conn(cluster, fun) end)
  end

  defp with_conn(cluster, fun) do
    case GenServer.call(cluster, :checkout) do
      {:ok, pool} ->
        fun.(pool)

      {:error, :empty} ->
        action = "checkout from cluster #{inspect(cluster)}"
        {:error, ConnectionError.new(action, {:cluster, :not_connected})}
    end
  end

  ## Callbacks and implementation stuff

  defguardp is_inet_port(port) when port in 0..65355
  defguardp is_ip(ip) when is_tuple(ip) and tuple_size(ip) in [4, 8]

  defguardp is_peername(peername)
            when is_tuple(peername) and tuple_size(peername) == 2 and is_ip(elem(peername, 0)) and
                   is_inet_port(elem(peername, 1))

  @impl true
  def init({%__MODULE__{} = state, nodes}) do
    # Start supervisor for the pools.
    {:ok, pool_sup} = Supervisor.start_link([], strategy: :one_for_one)

    {:ok, control_conn} =
      state.control_conn_mod.start_link(
        cluster: self(),
        contact_points: nodes,
        connection_options: state.pool_options,
        autodiscovered_nodes_port: state.autodiscovered_nodes_port
      )

    {:ok, %__MODULE__{state | pool_supervisor: pool_sup, control_connection: control_conn}}
  end

  @impl true
  def handle_call(:checkout, _from, %__MODULE__{} = state) do
    %__MODULE__{load_balancing: load_balancing, pools: pools} = state

    if Enum.empty?(pools) do
      {:reply, {:error, :empty}, state}
    else
      pool = select_pool(load_balancing, pools)
      {:reply, {:ok, pool}, state}
    end
  end

  @impl true
  def handle_info(msg, state)

  # The control connection discovered peers. The control connection doesn't keep track of
  # which peers it already notified the cluster of.
  def handle_info({:discovered_peers, peers}, %__MODULE__{} = state) do
    Logger.debug("Discoverer peers: #{inspect(peers)}")

    # Start a pool for each peer.
    state =
      Enum.reduce(peers, state, fn peer, state ->
        peer =
          case peer do
            peer when is_peername(peer) -> peer
            ip when is_ip(ip) -> {ip, state.autodiscovered_nodes_port}
            other -> raise("invalid peer #{inspect(other)}")
          end

        start_pool(state, peer)
      end)

    {:noreply, state}
  end

  def handle_info({:cluster_event, %StatusChange{} = status_change}, %__MODULE__{} = state) do
    state = handle_status_change(state, status_change)
    {:noreply, state}
  end

  def handle_info({:cluster_event, %TopologyChange{} = topology_change}, %__MODULE__{} = state) do
    state = handle_topology_change(state, topology_change)
    {:noreply, state}
  end

  ## Helpers

  # This function is idempotent: you can call it as many times as you want with the same
  # peer, and it'll only start it once.
  defp start_pool(state, {_ip, _port} = peer) when is_peername(peer) do
    conn_options = Keyword.put(state.pool_options, :nodes, [peername_to_string(peer)])

    pool_spec =
      Supervisor.child_spec({state.xandra_mod, conn_options}, id: peer, restart: :transient)

    case Supervisor.start_child(state.pool_supervisor, pool_spec) do
      {:ok, pool} ->
        Logger.debug("Started pool to: #{peername_to_string(peer)}")
        put_in(state.pools[peer], pool)

      {:error, :already_present} ->
        case Supervisor.restart_child(state.pool_supervisor, _id = peer) do
          {:ok, pool} ->
            Logger.debug("Restarted pool to: #{peername_to_string(peer)}")
            put_in(state.pools[peer], pool)

          {:error, reason} when reason in [:running, :restarting] ->
            state

          {:error, other} ->
            raise "unexpected error when restarting pool for #{peername_to_string(peer)}: #{inspect(other)}"
        end

      {:error, {:already_started, _pool}} ->
        state
    end
  end

  defp handle_status_change(state, %StatusChange{effect: "UP", address: address}) do
    start_pool(state, {address, state.autodiscovered_nodes_port})
  end

  defp handle_status_change(state, %StatusChange{effect: "DOWN", address: address}) do
    peername = {address, state.autodiscovered_nodes_port}
    Logger.debug("StatusChange DOWN for node: #{peername_to_string(peername)}")

    _ = Supervisor.terminate_child(state.pool_supervisor, peername)
    update_in(state.pools, &Map.delete(&1, peername))
  end

  defp handle_topology_change(state, %TopologyChange{effect: "NEW_NODE", address: address}) do
    peername = {address, state.autodiscovered_nodes_port}
    Logger.debug("TopologyChange NEW_NODE for node: #{peername_to_string(peername)}")

    # If we already know about this node, we ignore it. Otherwise, we start a pool for it.
    if Map.has_key?(state.pools, peername) do
      Logger.debug("Connection already established to node: #{peername_to_string(peername)}")
      state
    else
      start_pool(state, peername)
    end
  end

  defp handle_topology_change(state, %TopologyChange{effect: "REMOVED_NODE", address: address}) do
    peername = {address, state.autodiscovered_nodes_port}
    Logger.debug("TopologyChange REMOVED_NODE for node: #{peername_to_string(peername)}")

    # Terminate the pool and remove it from the supervisor.
    _ = Supervisor.terminate_child(state.pool_supervisor, peername)
    _ = Supervisor.delete_child(state.pool_supervisor, peername)

    update_in(state.pools, &Map.delete(&1, peername))
  end

  defp handle_topology_change(state, %TopologyChange{effect: "MOVED_NODE"} = event) do
    Logger.warn("Ignored TOPOLOGY_CHANGE event: #{inspect(event)}")
    state
  end

  defp select_pool(:random, pools) do
    {_address, pool} = Enum.random(pools)
    pool
  end

  defp select_pool(:priority, pools) do
    # TODO: this shouldn't work this way and we should actually figure it out
    {_address, pool} = Enum.at(pools, 0)
    pool
  end

  defp peername_to_string({ip, port} = peername) when is_peername(peername) do
    "#{:inet.ntoa(ip)}:#{port}"
  end
end
