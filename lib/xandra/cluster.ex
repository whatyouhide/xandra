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
  one `Xandra` connection or pool of connections will be started for each
  node specified in the `:nodes` option.

  The API provided by this module mirrors the API provided by the `Xandra`
  module. Queries executed through this module will be "routed" to nodes
  in the provided list of nodes based on a strategy. See the
  "Load balancing strategies" section below

  Note that regardless of the underlying pool, `Xandra.Cluster` will establish
  one extra connection to each node in the specified list of nodes (used for
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

  ## Load balancing strategies

  For now, there are two load balancing "strategies" implemented:

    * `:random` - it will choose one of the connected nodes at random and
      execute the query on that node.

    * `:priority` - it will choose a node to execute the query according
      to the order nodes appear in `:nodes`.

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
  alias Xandra.ConnectionError

  require Logger

  @type cluster :: GenServer.server()

  @default_load_balancing :random
  @default_port 9042

  @default_start_options [
    nodes: ["127.0.0.1"],
    idle_interval: 30_000,
    autodiscovery: true,
    autodiscovered_nodes_port: @default_port,
    max_autodiscovered_pools: 5
  ]

  defstruct [
    :options,
    :node_refs,
    :load_balancing,
    :autodiscovery,
    :autodiscovered_nodes_port,
    :max_autodiscovered_pools,
    :pool_supervisor,
    pools: %{}
  ]

  @doc """
  Starts a cluster connection.

  Note that a cluster connection starts an additional connection for each
  node in the cluster that is used for monitoring cluster updates.

  ## Options

  This function accepts all options accepted by `Xandra.start_link/1` and
  and forwards them to each connection or pool of connections. The following
  options are specific to this function:

    * `:load_balancing` - (atom) load balancing "strategy". Either `:random`
      or `:priority`. See the "Load balancing strategies" section above.
      Defaults to `:random`.

  ## Examples

  Starting a cluster connection and executing a query:

      {:ok, cluster} =
        Xandra.Cluster.start_link(
          nodes: ["cassandra1.example.net", "cassandra2.example.net"]
        )

  Passing options down to each connection:

      {:ok, cluster} =
        Xandra.Cluster.start_link(
          nodes: ["cassandra1.example.net", "cassandra2.example.net"],
          after_connect: &Xandra.execute!(&1, "USE my_keyspace")
        )

  """
  @spec start_link([Xandra.start_option() | {:load_balancing, atom}]) :: GenServer.on_start()
  def start_link(options) do
    options = Keyword.merge(@default_start_options, options)

    {load_balancing, options} = Keyword.pop(options, :load_balancing, @default_load_balancing)
    {nodes, options} = Keyword.pop(options, :nodes)
    {autodiscovery?, options} = Keyword.pop(options, :autodiscovery)
    {autodiscovered_nodes_port, options} = Keyword.pop(options, :autodiscovered_nodes_port)
    {max_autodiscovered_pools, options} = Keyword.pop(options, :max_autodiscovered_pools)
    {name, options} = Keyword.pop(options, :name)

    state = %__MODULE__{
      options: Keyword.delete(options, :pool),
      load_balancing: load_balancing,
      autodiscovery: autodiscovery?,
      autodiscovered_nodes_port: autodiscovered_nodes_port,
      max_autodiscovered_pools: max_autodiscovered_pools
    }

    nodes = Enum.map(nodes, &parse_node/1)

    GenServer.start_link(__MODULE__, {state, nodes}, name: name)
  end

  # Used internally by Xandra.Cluster.ControlConnection.
  @doc false
  def activate(cluster, node_ref, address, port) do
    GenServer.cast(cluster, {:activate, node_ref, address, port})
  end

  # Used internally by Xandra.Cluster.ControlConnection.
  @doc false
  def update(cluster, status_change) do
    GenServer.cast(cluster, {:update, status_change})
  end

  # Used internally by Xandra.Cluster.ControlConnection.
  @doc false
  def discovered_peers(cluster, peers) do
    GenServer.cast(cluster, {:discovered_peers, peers})
  end

  @doc """
  Same as `Xandra.stream_pages!/4`.
  """
  @spec stream_pages!(
          cluster,
          Xandra.statement() | Xandra.Prepared.t(),
          Xandra.values(),
          keyword
        ) ::
          Enumerable.t()
  def stream_pages!(conn, query, params, options \\ []) do
    Xandra.stream_pages!(conn, query, params, options)
  end

  @doc """
  Same as `Xandra.prepare/3`.
  """
  @spec prepare(cluster, Xandra.statement(), keyword) ::
          {:ok, Xandra.Prepared.t()} | {:error, Xandra.error()}
  def prepare(cluster, statement, options \\ []) when is_binary(statement) do
    with_conn(cluster, &Xandra.prepare(&1, statement, options))
  end

  @doc """
  Same as `Xandra.prepare!/3`.
  """
  @spec prepare!(cluster, Xandra.statement(), keyword) :: Xandra.Prepared.t() | no_return
  def prepare!(cluster, statement, options \\ []) do
    case prepare(cluster, statement, options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Same as `Xandra.execute/3`.
  """
  @spec execute(cluster, Xandra.statement() | Xandra.Prepared.t(), Xandra.values()) ::
          {:ok, Xandra.result()} | {:error, Xandra.error()}
  @spec execute(cluster, Xandra.Batch.t(), keyword) ::
          {:ok, Xandra.Void.t()} | {:error, Xandra.error()}
  def execute(cluster, query, params_or_options \\ []) do
    with_conn(cluster, &Xandra.execute(&1, query, params_or_options))
  end

  @doc """
  Same as `Xandra.execute/4`.
  """
  @spec execute(cluster, Xandra.statement() | Xandra.Prepared.t(), Xandra.values(), keyword) ::
          {:ok, Xandra.result()} | {:error, Xandra.error()}
  def execute(cluster, query, params, options) do
    with_conn(cluster, &Xandra.execute(&1, query, params, options))
  end

  @doc """
  Same as `Xandra.execute!/3`.
  """
  @spec execute(cluster, Xandra.statement() | Xandra.Prepared.t(), Xandra.values()) ::
          Xandra.result() | no_return
  @spec execute(cluster, Xandra.Batch.t(), keyword) ::
          Xandra.Void.t() | no_return
  def execute!(cluster, query, params_or_options \\ []) do
    case execute(cluster, query, params_or_options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Same as `Xandra.execute!/4`.
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
  Same as `Xandra.run/3`.

  The connection that is passed to `fun` is a Xandra connection, not a
  cluster. This means that you should call `Xandra` functions on it.

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
  def init({%__MODULE__{options: options} = state, nodes}) do
    {:ok, pool_supervisor} = Supervisor.start_link([], strategy: :one_for_one, max_restarts: 0)
    node_refs = start_control_connections(nodes, options, state.autodiscovery)
    {:ok, %{state | node_refs: node_refs, pool_supervisor: pool_supervisor}}
  end

  @impl true
  def handle_call(:checkout, _from, %__MODULE__{} = state) do
    %{
      node_refs: node_refs,
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

  def handle_cast({:activate, node_ref, address, port}, %__MODULE__{} = state) do
    _ = Logger.debug("Control connection for #{:inet.ntoa(address)}:#{port} is up")

    # Update the node_refs with the actual address of the control connection node.
    state = update_in(state.node_refs, &List.keystore(&1, node_ref, 0, {node_ref, address}))

    state = start_pool(state, address, port)
    {:noreply, state}
  end

  def handle_cast({:discovered_peers, peers}, %__MODULE__{} = state) do
    _ = Logger.debug("Discovered peers: #{inspect(peers)}")
    port = state.autodiscovered_nodes_port
    state = Enum.reduce(peers, state, &start_pool(_state = &2, _peer = &1, port))
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

  ## Helpers

  defp start_control_connections(nodes, options, autodiscovery?) do
    cluster = self()

    Enum.map(nodes, fn {address, port} ->
      node_ref = make_ref()
      ControlConnection.start_link(cluster, node_ref, address, port, options, autodiscovery?)
      {node_ref, nil}
    end)
  end

  defp start_pool(state, address, port) do
    %{
      options: options,
      pool_supervisor: pool_supervisor,
      pools: pools
    } = state

    options = Keyword.merge(options, address: address, port: port)
    child_spec = Supervisor.child_spec({Xandra, options}, id: address)

    case Supervisor.start_child(pool_supervisor, child_spec) do
      {:ok, pool} ->
        _ = Logger.debug("Started connection to #{inspect(address)}")
        %{state | pools: Map.put(pools, address, pool)}

      {:error, {:already_started, _pool}} ->
        # TODO: to have a reliable cluster name, we need to bring the name given on
        # start_link/1 into the state because it could be an atom, {:global, term}
        # and so on.
        Logger.warn(fn ->
          "Xandra cluster #{inspect(self())} " <>
            "received request to start another connection pool " <>
            "to the same address: #{inspect(address)}"
        end)

        state
    end
  end

  defp handle_status_change(state, %{effect: "UP", address: address}) do
    %{pool_supervisor: pool_supervisor, pools: pools} = state

    case Supervisor.restart_child(pool_supervisor, address) do
      {:error, reason} when reason in [:not_found, :running, :restarting] ->
        state

      {:ok, pool} ->
        %{state | pools: Map.put(pools, address, pool)}
    end
  end

  defp handle_status_change(state, %{effect: "DOWN", address: address}) do
    %{pool_supervisor: pool_supervisor, pools: pools} = state

    _ = Supervisor.terminate_child(pool_supervisor, address)
    %{state | pools: Map.delete(pools, address)}
  end

  defp handle_topology_change(state, %{effect: "NEW_NODE", address: address}) do
    start_pool(state, address, state.autodiscovered_nodes_port)
  end

  defp handle_topology_change(state, %{effect: "REMOVED_NODE", address: address}) do
    %{pool_supervisor: pool_supervisor, pools: pools} = state
    _ = Supervisor.terminate_child(pool_supervisor, address)
    _ = Supervisor.delete_child(pool_supervisor, address)
    %{state | pools: Map.delete(pools, address)}
  end

  defp handle_topology_change(state, %{effect: "MOVED_NODE"} = event) do
    _ = Logger.warn("Ignored TOPOLOGY_CHANGE event: #{inspect(event)}")
    state
  end

  defp select_pool(:random, pools, _node_refs) do
    {_address, pool} = Enum.random(pools)
    pool
  end

  defp select_pool(:priority, pools, node_refs) do
    Enum.find_value(node_refs, fn {_node_ref, address} ->
      Map.get(pools, address)
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
