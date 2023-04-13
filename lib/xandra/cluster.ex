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

  ## Child specification

  `Xandra.Cluster` implements a `child_spec/1` function, so it can be used as a child
  under a supervisor:

      children = [
        # ...,
        {Xandra.Cluster, nodes: ["cassandra-seed.example.net"]}
      ]

  ## Contact points and cluster discovery

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

  ## Load-balancing policies

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

  ## Disconnections and reconnections

  `Xandra.Cluster` also supports nodes disconnecting and reconnecting: if Xandra
  detects one of the nodes in `:nodes` going down, it will not execute queries
  against it anymore, but will start executing queries on it as soon as it
  detects such node is back up.

  If all specified nodes happen to be down when a query is executed, a
  `Xandra.ConnectionError` with reason `{:cluster, :not_connected}` will be
  returned.

  ## Telemetry

  This section describes all the Telemetry events that `Xandra.Cluster` emits. These events
  are available since *v0.15.0*. See also `Xandra.Telemetry`.

  * `[:xandra, :cluster, :change_event]` — emitted when there is a change in the
    cluster, either as reported by Cassandra itself or as detected by Xandra.
    **Measurements**: *none*.
    **Metadata**:
      * `:event_type` - one of `:host_up` (a host went up), `:host_down` (a host went down),
        `:host_added` (a host was added to the cluster topology), or `:host_removed` (a host
        was removed from the cluster topology).
      * `:source` - one of `:cassandra` or `:xandra`. If the event was reported by
        Cassandra itself, the source is `:cassandra`. If the event was detected by
        Xandra, the source is `:xandra`.
      * `:changed` (`t:boolean/0`) - this is `true` if the node wasn't in the state
        reported by the event, and `false` if the node was already in the reported state.
      * `:host` (`t:Xandra.Cluster.Host.t/0`) - the host that went up or down.
      * `:cluster_pid` (`t:pid/0`) - the PID of the cluster process.
      * `:cluster_name` - the name of the cluster executing the event, if provided
        through the `:name` option in `start_link/1`. `nil` if no `:name` was provided.

  * `[:xandra, :cluster, :control_connection, :connected]` — emitted when the control
    connection for the cluster is established.
    **Measurements**: *none*.
    **Metadata**:
      * `:host` (`t:Xandra.Cluster.Host.t/0`) - the host that the control connection is
        connected to.
      * `:cluster_pid` (`t:pid/0`) - the PID of the cluster process.
      * `:cluster_name` - the name of the cluster executing the event, if provided
        through the `:name` option in `start_link/1`. `nil` if no `:name` was provided.

  * `[:xandra, :cluster, :control_connection, :disconnected]` — emitted when the control
    connection for the cluster is established.
    **Measurements**: *none*.
    **Metadata**:
      * `:host` (`t:Xandra.Cluster.Host.t/0`) - the host that the control connection is
        connected to.
      * `:reason` - the reason for the disconnection. For example, `:closed` if the connected
        node closes the connection peacefully.
      * `:cluster_pid` (`t:pid/0`) - the PID of the cluster process.
      * `:cluster_name` - the name of the cluster executing the event, if provided
        through the `:name` option in `start_link/1`. `nil` if no `:name` was provided.

  """

  use GenServer

  alias Xandra.Cluster.{
    ControlConnection,
    LoadBalancingPolicy,
    Host
  }

  alias Xandra.{Batch, ConnectionError, Prepared, RetryStrategy}

  require Logger
  require Record

  @type cluster :: GenServer.server()

  @default_port 9042

  # State.
  defstruct [
    # Options for the underlying connection pools.
    :pool_options,

    # When auto-discovering nodes, you cannot get their port from C*.
    # Other drivers solve this by providing a static port that the driver
    # uses to connect to any autodiscovered node.
    :autodiscovered_nodes_port,

    # A supervisor that supervises pools.
    :pool_supervisor,

    # The PID of the control connection.
    :control_connection,

    # The load balancing policy info.
    :load_balancing_module,
    :load_balancing_state,

    # The registry where connections are registered.
    :registry,

    # The number of target pools.
    :target_pools,

    # A map of peername to pool PID pairs.
    pools: %{},

    # Modules to swap processes when testing.
    xandra_mod: nil,
    control_conn_mod: nil
  ]

  @start_link_opts_schema [
    nodes: [
      type: {:list, {:custom, Xandra.OptionsValidators, :validate_node, []}},
      default: ["127.0.0.1"],
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
      type: {:or, [{:in, [:priority, :random]}, :mod_arg]},
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
      # TODO: use type_doc: "`t::inet.port_number/0`" when we depend on nimble_options 1.0.
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
      The number of nodes to start pools to. Each pool will use the `:pool_size` option
      (see `Xandra.start_link/1`) to determine how many single connections to open to that
      node. This number is a *target* number, which means that sometimes there might not
      be enough nodes to start this many pools. Xandra won't ever start more than
      `:target_pools` pools. *Available since v0.15.0*.
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
          after_connect: &Xandra.execute!(&1, "USE my_keyspace")
        )

  """
  @spec start_link([option]) :: GenServer.on_start()
        when option: Xandra.start_option() | start_option()
  def start_link(options) when is_list(options) do
    {cluster_opts, pool_opts} = Keyword.split(options, @start_link_opts_schema_keys)
    cluster_opts = NimbleOptions.validate!(cluster_opts, @start_link_opts_schema)

    GenServer.start_link(
      __MODULE__,
      {cluster_opts, pool_opts},
      Keyword.take(cluster_opts, [:name])
    )
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
  on the load-balancing policy given in `start_link/1`.

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

  @doc """
  Synchronously stops the given cluster with the given reason.

  Waits `timeout` milliseconds for the cluster to stop before aborting and exiting.
  """
  @doc since: "0.15.0"
  @spec stop(cluster, term, timeout) :: :ok
  def stop(cluster, reason \\ :normal, timeout \\ :infinity)
      when timeout == :infinity or (is_integer(timeout) and timeout >= 0) do
    GenServer.stop(cluster, reason, timeout)
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

  @impl true
  def init({cluster_opts, pool_opts}) do
    {nodes, cluster_opts} = Keyword.pop!(cluster_opts, :nodes)

    registry_name =
      Module.concat([Xandra.ClusterRegistry, to_string(System.unique_integer([:positive]))])

    {:ok, _} = Registry.start_link(keys: :unique, name: registry_name)

    {lb_mod, lb_opts} =
      case Keyword.fetch!(cluster_opts, :load_balancing) do
        :random -> {LoadBalancingPolicy.Random, []}
        :priority -> raise "not implemented yet"
        {mod, opts} -> {mod, opts}
      end

    state = %__MODULE__{
      pool_options: pool_opts,
      load_balancing_module: lb_mod,
      load_balancing_state: lb_mod.init(lb_opts),
      autodiscovered_nodes_port: Keyword.fetch!(cluster_opts, :autodiscovered_nodes_port),
      xandra_mod: Keyword.fetch!(cluster_opts, :xandra_module),
      control_conn_mod: Keyword.fetch!(cluster_opts, :control_connection_module),
      target_pools: Keyword.fetch!(cluster_opts, :target_pools),
      registry: registry_name
    }

    # Start supervisor for the pools.
    {:ok, pool_sup} = Supervisor.start_link([], strategy: :one_for_one)

    {:ok, control_conn} =
      state.control_conn_mod.start_link(
        cluster: self(),
        contact_points: nodes,
        connection_options: state.pool_options,
        autodiscovered_nodes_port: state.autodiscovered_nodes_port,
        load_balancing: {lb_mod, lb_opts},
        refresh_topology_interval: Keyword.fetch!(cluster_opts, :refresh_topology_interval),
        registry: registry_name,
        name: Keyword.get(cluster_opts, :name)
      )

    state = %__MODULE__{state | pool_supervisor: pool_sup, control_connection: control_conn}
    {:ok, state}
  end

  @impl true
  def handle_call(:checkout, _from, %__MODULE__{} = state) do
    {hosts_plan, state} =
      get_and_update_in(state.load_balancing_state, fn lb_state ->
        state.load_balancing_module.hosts_plan(lb_state)
      end)

    # Find the first host in the plan for which we have a pool.
    reply =
      hosts_plan
      |> Stream.map(fn %Host{} = host -> Map.fetch(state.pools, Host.to_peername(host)) end)
      |> Enum.find(_default = {:error, :empty}, &match?({:ok, _}, &1))

    {:reply, reply, state}
  end

  @impl true
  def handle_info(msg, state)

  def handle_info({:host_up, %Host{} = host}, %__MODULE__{} = state) do
    Logger.debug("Host marked as UP: #{Host.format_address(host)}")
    state = update_in(state.load_balancing_state, &state.load_balancing_module.host_up(&1, host))
    state = maybe_start_pools(state)
    {:noreply, state}
  end

  def handle_info({:host_down, %Host{} = host}, %__MODULE__{} = state) do
    Logger.debug("Host marked as DOWN: #{Host.format_address(host)}")

    state =
      update_in(state.load_balancing_state, &state.load_balancing_module.host_down(&1, host))

    state = stop_pool(state, host)
    state = maybe_start_pools(state)
    {:noreply, state}
  end

  def handle_info({:host_added, %Host{} = host}, %__MODULE__{} = state) do
    Logger.debug("Host added to the cluster: #{Host.format_address(host)}")

    state =
      update_in(state.load_balancing_state, &state.load_balancing_module.host_added(&1, host))

    state = maybe_start_pools(state)
    {:noreply, state}
  end

  def handle_info({:host_removed, %Host{} = host}, %__MODULE__{} = state) do
    Logger.debug("Host removed from the cluster: #{Host.format_address(host)}")
    state = stop_pool(state, host)

    # Also delete the child from the supervisor altogether.
    _ = Supervisor.delete_child(state.pool_supervisor, {host.address, host.port})

    state =
      update_in(state.load_balancing_state, &state.load_balancing_module.host_removed(&1, host))

    state = update_in(state.pools, &Map.delete(&1, {host.address, host.port}))

    state = maybe_start_pools(state)
    {:noreply, state}
  end

  def handle_info({:discovered_hosts, hosts}, %__MODULE__{} = state) when is_list(hosts) do
    Logger.debug("Discovered hosts: #{Enum.map_join(hosts, ", ", &Host.format_address/1)}")

    state =
      Enum.reduce(hosts, state, fn %Host{} = host, acc ->
        update_in(acc.load_balancing_state, &state.load_balancing_module.host_added(&1, host))
      end)

    state = maybe_start_pools(state)
    {:noreply, state}
  end

  ## Helpers

  # This function is idempotent: you can call it as many times as you want with the same
  # peer, and it'll only start it once.
  defp start_pool(state, %Host{} = host) do
    conn_options =
      Keyword.merge(state.pool_options,
        nodes: [Host.format_address(host)],
        registry: state.registry,
        connection_listeners: [state.control_connection]
      )

    peername = Host.to_peername(host)

    pool_spec =
      Supervisor.child_spec({state.xandra_mod, conn_options},
        id: peername,
        restart: :transient
      )

    case Supervisor.start_child(state.pool_supervisor, pool_spec) do
      {:ok, pool} ->
        Logger.debug("Started pool to: #{Host.format_address(host)}")
        put_in(state.pools[peername], pool)

      {:error, :already_present} ->
        case Supervisor.restart_child(state.pool_supervisor, _id = peername) do
          {:ok, pool} ->
            Logger.debug("Restarted pool to: #{Host.format_address(host)}")
            put_in(state.pools[peername], pool)

          {:error, reason} when reason in [:running, :restarting] ->
            state

          {:error, other} ->
            raise "unexpected error when restarting pool for #{Host.format_address(host)}: #{inspect(other)}"
        end

      {:error, {:already_started, _pool}} ->
        state
    end
  end

  defp stop_pool(state, %Host{} = host) do
    _ = Supervisor.terminate_child(state.pool_supervisor, {host.address, host.port})
    update_in(state.pools, &Map.delete(&1, {host.address, host.port}))
  end

  defp maybe_start_pools(%__MODULE__{target_pools: target, pools: pools} = state)
       when map_size(pools) == target do
    state
  end

  defp maybe_start_pools(%__MODULE__{target_pools: target, pools: pools} = state)
       when map_size(pools) < target do
    {hosts_plan, state} =
      get_and_update_in(state.load_balancing_state, fn lb_state ->
        state.load_balancing_module.hosts_plan(lb_state)
      end)

    Enum.reduce_while(hosts_plan, state, fn %Host{} = host, state ->
      case Map.fetch(pools, Host.to_peername(host)) do
        {:ok, _pool} ->
          {:cont, state}

        :error ->
          state = start_pool(state, host)

          if map_size(state.pools) == target do
            {:halt, state}
          else
            {:cont, state}
          end
      end
    end)
  end
end
