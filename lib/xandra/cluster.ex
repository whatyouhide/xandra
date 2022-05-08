defmodule Xandra.Cluster do
  @moduledoc """
  Connection to a Cassandra cluster.

  This module is a "proxy" connection with support for connecting to multiple
  nodes in a Cassandra cluster and executing queries on such nodes based on a
  given *strategy*.

  ## Usage

  This module manages connections to different nodes in a Cassandra cluster.
  Each connection to a node is a `Xandra` connection (so it can also be
  a pool of connections). When a `Xandra.Cluster` connection is started,
  one `Xandra` pool of connections will be started for each node specified
  in the `:nodes` option plus for autodiscovered nodes if the `:autodiscovery`
  option is `true`.

  The API provided by this module mirrors the API provided by the `Xandra`
  module. Queries executed through this module will be "routed" to nodes
  in the provided list of nodes based on a strategy. See the
  "Load balancing strategies" section below

  Note that regardless of the underlying pool, `Xandra.Cluster` will establish
  one extra connection to each node in the specified list of `:nodes` (used for
  internal purposes).

  Here is an example of how one could use `Xandra.Cluster` to connect to
  multiple nodes:

      Xandra.Cluster.start_link(
        nodes: ["cassandra1.example.net", "cassandra2.example.net"],
        pool_size: 10,
      )

  The code above will establish a pool of ten connections to each of the nodes
  specified in `:nodes`, for a total of twenty connections going out of the
  current machine, plus two extra connections (one per node) used for internal
  purposes.

  ## Autodiscovery

  When the `:autodiscovery` option is `true` (which is the default),
  `Xandra.Cluster` discovers nodes in the same cluster as the nodes
  specified in the `:nodes` option. The nodes in `:nodes` act as "seed"
  nodes. When nodes in the cluster are discovered, a `Xandra` pool of
  connections is started for each node that is in the **same datacenter**
  as one of the nodes in `:nodes`. For now, there is no limit on how many
  nodes in the same datacenter `Xandra.Cluster` discovers and connects to.

  As mentioned before, a "control connection" for internal purposes is established
  to each node in `:nodes`. These control connections are *not* established for
  autodiscovered nodes. This means that if you only have one seed node in `:nodes`,
  there will only be one control connection: if that control connection goes down
  for some reason, you won't receive cluster change events anymore. This will cause
  disconnections but will not technically break anything.

  ## Load balancing strategies

  For now, there are two load balancing "strategies" implemented:

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
  alias Xandra.{Batch, ConnectionError, Prepared, Protocol, RetryStrategy}

  require Logger

  @type cluster :: GenServer.server()

  @default_load_balancing :random
  @default_port 9042

  @default_start_options [
    nodes: ["127.0.0.1"],
    idle_interval: 30_000,
    protocol_version: :v3,
    autodiscovery: true,
    autodiscovered_nodes_port: @default_port
  ]

  defstruct [
    # Options for the cluster and for the underlying connection pools.
    :options,

    # Load balancing strategy.
    :load_balancing,

    # A boolean that decides whether to discover new nodes in the cluster
    # and add them to the pool.
    :autodiscovery,

    # When autodiscovering nodes, you cannot get their port from C*.
    # Other drivers solve this by providing a static port that the driver
    # uses to connect to any autodiscovered node.
    :autodiscovered_nodes_port,

    # A supervisor that supervises pools.
    :pool_supervisor,

    # A supervisor that supervises control connections.
    # Children under this supervisor are identified by a "node_ref"
    # (t:reference/0) generated when we start each control connection.
    # We keep a reverse lookup of {peername, node_ref} pairs in the
    # :control_conn_peername_to_node_ref key.
    :control_conn_supervisor,

    # A map of peername to pool PID pairs.
    pools: %{},

    # A reverse lookup list of peername => node_ref. This is used
    # to retrieve the node_ref of a control connection in order to
    # operate that control connection under the control connection
    # supervisor. The reason this is a list is that we want to keep
    # it ordered in order to support the :priority strategy,
    # which runs a query through the same order of nodes every time.
    control_conn_peername_to_node_ref: [],

    # Modules to swap processes when testing.
    xandra_mod: nil,
    control_conn_mod: nil
  ]

  @doc """
  Starts a cluster connection.

  Note that a cluster connection starts an additional connection for each
  node specified in `:nodes`. Such "control connection" is used for monitoring
  cluster updates.

  ## Options

  This function accepts all options accepted by `Xandra.start_link/1` and
  and forwards them to each connection or pool of connections. The following
  options are specific to this function:

    * `:load_balancing` - (atom) load balancing "strategy". Either `:random`
      or `:priority`. See the "Load balancing strategies" section in the module
      documentation. If `:autodiscovery` is `true`, the only supported strategy
      is `:random`. Defaults to `:random`.

    * `:nodes` - (list of strings) a list of nodes to use as seed nodes
      when setting up the cluster. The behaviour of this option depends on
      the `:autodiscovery` option. See the "Autodiscovery" section below.
      If the `:autodiscovery` option is `false`, the cluster only connects
      to the nodes in `:nodes` and sets up one additional control connection
      for each one of these nodes. Defaults to `["127.0.0.1"]`.

    * `:autodiscovery` - (boolean) whether to autodiscover nodes in the
      cluster. See the "Autodiscovery" section in the module documentation.
      Defaults to `true`.

    * `:autodiscovered_nodes_port` - (integer) the port to use when connecting
      to autodiscovered nodes. Cassandra does not advertise the port of nodes
      when discovering them, so you'll need to specify one explicitly. This might
      get fixed in future Cassandra versions. Defaults to `9042`.

  ## Examples

  Starting a cluster connection to two specific nodes in the cluster:

      {:ok, cluster} =
        Xandra.Cluster.start_link(
          nodes: ["cassandra1.example.net", "cassandra2.example.net"],
          autodiscovery: false
        )

  Starting a pool of five connections to nodes in the same cluster as the given
  "seed" node:

      {:ok, cluster} =
        Xandra.Cluster.start_link(
          autodiscovery: true,
          nodes: ["cassandra-seed.example.net"]
          pool_size: 5
        )

  Passing options down to each connection:

      {:ok, cluster} =
        Xandra.Cluster.start_link(
          nodes: ["cassandra.example.net"],
          after_connect: &Xandra.execute!(&1, "USE my_keyspace")
        )

  """
  @spec start_link([Xandra.start_option() | {:load_balancing, atom}]) :: GenServer.on_start()
  def start_link(options) do
    options = Keyword.merge(@default_start_options, options)

    # We don't pop the :protocol_version option because we want to
    # also forward it to the Xandra connections.
    options =
      Keyword.put(
        options,
        :protocol_module,
        protocol_version_to_module(options[:protocol_version])
      )

    # These are mostly used for swapping the real ones with test processes.
    {xandra_mod, options} = Keyword.pop(options, :xandra_module, Xandra)

    {control_conn_mod, options} =
      Keyword.pop(options, :control_connection_module, ControlConnection)

    {load_balancing, options} = Keyword.pop(options, :load_balancing, @default_load_balancing)
    {nodes, options} = Keyword.pop(options, :nodes)
    {autodiscovery?, options} = Keyword.pop(options, :autodiscovery)
    {autodiscovered_nodes_port, options} = Keyword.pop(options, :autodiscovered_nodes_port)
    {name, options} = Keyword.pop(options, :name)

    if autodiscovery? and load_balancing == :priority do
      raise ArgumentError,
            "the :priority load balancing strategy is only supported when :autodiscovery is false"
    end

    state = %__MODULE__{
      options: Keyword.delete(options, :pool),
      load_balancing: load_balancing,
      autodiscovery: autodiscovery?,
      autodiscovered_nodes_port: autodiscovered_nodes_port,
      xandra_mod: xandra_mod,
      control_conn_mod: control_conn_mod
    }

    nodes = Enum.map(nodes, &parse_node/1)

    GenServer.start_link(__MODULE__, {state, nodes}, name: name)
  end

  # Used internally by Xandra.Cluster.ControlConnection.
  @doc false
  def activate(cluster, node_ref, {_ip, _port} = peername) do
    GenServer.cast(cluster, {:activate, node_ref, peername})
  end

  # Used internally by Xandra.Cluster.ControlConnection.
  @doc false
  def update(cluster, status_change) do
    GenServer.cast(cluster, {:update, status_change})
  end

  # Used internally by Xandra.Cluster.ControlConnection.
  @doc false
  def discovered_peers(cluster, peers, source_control_conn) do
    GenServer.cast(cluster, {:discovered_peers, peers, source_control_conn})
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

  ## Callbacks

  @impl true
  def init({%__MODULE__{} = state, nodes}) do
    control_conn_children = Enum.map(nodes, &control_conn_child_spec(&1, state))

    {:ok, control_conn_sup} = Supervisor.start_link(control_conn_children, strategy: :one_for_one)

    {:ok, pool_sup} = Supervisor.start_link([], strategy: :one_for_one)

    node_refs = for %{id: ref} <- control_conn_children, do: {_peername = nil, ref}

    state = %__MODULE__{
      state
      | control_conn_supervisor: control_conn_sup,
        pool_supervisor: pool_sup,
        control_conn_peername_to_node_ref: node_refs
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:checkout, _from, %__MODULE__{} = state) do
    %__MODULE__{
      control_conn_peername_to_node_ref: node_refs,
      load_balancing: load_balancing,
      pools: pools
    } = state

    if Enum.empty?(pools) do
      {:reply, {:error, :empty}, state}
    else
      pool = select_pool(load_balancing, pools, node_refs)
      {:reply, {:ok, pool}, state}
    end
  end

  @impl true
  def handle_cast(message, state)

  # A control connection came online.
  def handle_cast({:activate, node_ref, {_ip, _port} = peername}, %__MODULE__{} = state) do
    _ = Logger.debug("Control connection for #{peername_to_string(peername)} is up")

    # Check whether we already had an active control connection to this peer.
    # If we did, shut down the control connection that just reported active.
    # Otherwise, store this control connection and start the pool for this
    # peer.
    if List.keymember?(state.control_conn_peername_to_node_ref, peername, 0) do
      Logger.debug(
        "Control connection for #{peername_to_string(peername)} was already present, shutting this one down"
      )

      state =
        update_in(state.control_conn_peername_to_node_ref, fn list ->
          List.keydelete(list, node_ref, 1)
        end)

      _ = Supervisor.terminate_child(state.control_conn_supervisor, node_ref)
      _ = Supervisor.delete_child(state.control_conn_supervisor, node_ref)
      {:noreply, state}
    else
      # Store the peername alongside the original node_ref that we kept.
      state =
        update_in(state.control_conn_peername_to_node_ref, fn list ->
          List.keystore(list, node_ref, 1, {peername, node_ref})
        end)

      state = start_pool(state, node_ref, peername)
      {:noreply, state}
    end
  end

  def handle_cast({:discovered_peers, peers, source_control_conn}, %__MODULE__{} = state) do
    _ =
      Logger.debug(fn ->
        "Discovered peers from #{inspect(source_control_conn)}: " <>
          inspect(Enum.map(peers, &:inet.ntoa/1))
      end)

    port = state.autodiscovered_nodes_port

    state =
      Enum.reduce(peers, state, fn ip, acc ->
        peername = {ip, port}

        # Ignore this peer if we already had a control connection (and
        # thus a pool) for it.
        if List.keymember?(state.control_conn_peername_to_node_ref, peername, 0) do
          Logger.debug("Connection to node #{peername_to_string({ip, port})} already established")
          acc
        else
          control_conn_spec = control_conn_child_spec(peername, state)
          node_ref = control_conn_spec.id

          # Append this node_ref (and later on its peername) to the ordered
          # list of node_refs.
          acc =
            update_in(acc.control_conn_peername_to_node_ref, fn list ->
              List.keystore(list, node_ref, 1, {_peername = nil, node_ref})
            end)

          {:ok, _pid} = Supervisor.start_child(state.control_conn_supervisor, control_conn_spec)

          acc
        end
      end)

    {:noreply, state}
  end

  def handle_cast({:update, {:control_connection_established, address}}, %__MODULE__{} = state) do
    state = restart_pool(state, address)
    {:noreply, state}
  end

  def handle_cast({:update, %StatusChange{} = status_change}, %__MODULE__{} = state) do
    state = handle_status_change(state, status_change)
    {:noreply, state}
  end

  def handle_cast({:update, %TopologyChange{} = topology_change}, %__MODULE__{} = state) do
    state = handle_topology_change(state, topology_change)
    {:noreply, state}
  end

  @impl true
  def handle_info(message, state) do
    Logger.debug("Received message #{inspect(message)} with state: #{inspect(state)}")
    {:noreply, state}
  end

  ## Helpers

  defp control_conn_child_spec({address, port}, %__MODULE__{} = state) do
    %__MODULE__{
      autodiscovery: autodiscovery?,
      options: options,
      control_conn_mod: control_conn_mod
    } = state

    node_ref = make_ref()
    start_args = [_cluster = self(), node_ref, address, port, options, autodiscovery?]
    Supervisor.child_spec({control_conn_mod, start_args}, id: node_ref, restart: :transient)
  end

  defp start_pool(%__MODULE__{} = state, _node_ref, {ip, port} = peername) do
    options = Keyword.merge(state.options, address: ip, port: port)

    pool_spec =
      Supervisor.child_spec({state.xandra_mod, options}, id: peername, restart: :transient)

    # TODO: handle other return values
    case Supervisor.start_child(state.pool_supervisor, pool_spec) do
      {:ok, pool} ->
        _ = Logger.debug("Started connection pool to #{peername_to_string(peername)}")
        put_in(state.pools[peername], pool)
    end
  end

  defp restart_pool(state, address) do
    Logger.debug("Restarting pool: #{inspect(address)}")
    %__MODULE__{pool_supervisor: pool_supervisor, pools: pools} = state

    case Supervisor.restart_child(pool_supervisor, address) do
      {:error, reason} when reason in [:not_found, :running, :restarting] ->
        state

      {:ok, pool} ->
        %__MODULE__{state | pools: Map.put(pools, address, pool)}
    end
  end

  defp handle_status_change(state, %StatusChange{effect: "UP", address: address}) do
    restart_pool(state, {address, state.autodiscovered_nodes_port})
  end

  defp handle_status_change(state, %StatusChange{effect: "DOWN", address: address}) do
    Logger.debug("StatusChange DOWN for node: #{inspect(address)}")
    peername = {address, state.autodiscovered_nodes_port}
    %__MODULE__{pool_supervisor: pool_supervisor, pools: pools} = state
    _ = Supervisor.terminate_child(pool_supervisor, peername)
    %__MODULE__{state | pools: Map.delete(pools, peername)}
  end

  # We don't care about changes in the topology if we're not autodiscovering
  # nodes.
  defp handle_topology_change(%__MODULE__{autodiscovery: false} = state, %TopologyChange{}) do
    state
  end

  defp handle_topology_change(state, %TopologyChange{effect: "NEW_NODE", address: address}) do
    peername = {address, state.autodiscovered_nodes_port}

    # Ignore this peer if we already had a control connection (and
    # thus a pool) for it.
    if List.keymember?(state.control_conn_peername_to_node_ref, peername, 0) do
      Logger.debug("Connection to node #{peername_to_string(peername)} already established")
      state
    else
      control_conn_spec = control_conn_child_spec(peername, state)
      node_ref = control_conn_spec.id

      # Append this node_ref (and later on its peername) to the ordered
      # list of node_refs.
      state =
        update_in(state.control_conn_peername_to_node_ref, fn list ->
          List.keystore(list, node_ref, 1, {_peername = nil, node_ref})
        end)

      {:ok, _pid} = Supervisor.start_child(state.control_conn_supervisor, control_conn_spec)

      state
    end
  end

  defp handle_topology_change(state, %TopologyChange{effect: "REMOVED_NODE", address: address}) do
    %__MODULE__{
      pool_supervisor: pool_supervisor,
      pools: pools,
      control_conn_supervisor: control_conn_supervisor
    } = state

    peername = {address, state.autodiscovered_nodes_port}

    # Terminate the pool and remove it from the supervisor.
    _ = Supervisor.terminate_child(pool_supervisor, peername)
    _ = Supervisor.delete_child(pool_supervisor, peername)

    # Terminate the control connection and remove it from the supervisor.

    {ref, state} =
      get_and_update_in(state.control_conn_peername_to_node_ref, fn list ->
        # TODO: Replace with List.keyfind!/3 when we depend on Elixir 1.13+.
        {^peername, ref} = List.keyfind(list, peername, 0)
        {ref, List.keydelete(list, peername, 0)}
      end)

    _ = Supervisor.terminate_child(control_conn_supervisor, ref)
    _ = Supervisor.delete_child(control_conn_supervisor, ref)

    %__MODULE__{state | pools: Map.delete(pools, peername)}
  end

  defp handle_topology_change(state, %TopologyChange{effect: "MOVED_NODE"} = event) do
    _ = Logger.warn("Ignored TOPOLOGY_CHANGE event: #{inspect(event)}")
    state
  end

  defp select_pool(:random, pools, _node_refs) do
    {_address, pool} = Enum.random(pools)
    pool
  end

  defp select_pool(:priority, pools, node_refs) do
    Enum.find_value(node_refs, fn {peername, _node_ref} -> Map.get(pools, peername) end)
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

  defp protocol_version_to_module(:v3), do: Protocol.V3

  defp protocol_version_to_module(:v4), do: Protocol.V4

  defp protocol_version_to_module(other),
    do: raise(ArgumentError, "unknown protocol version: #{inspect(other)}")

  defp peername_to_string({ip, port}) do
    "#{:inet.ntoa(ip)}:#{port}"
  end
end
