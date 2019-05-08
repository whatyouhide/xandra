defmodule Xandra.Cluster do
  @moduledoc """
  A pool that implements clustering support.

  This module is a pool that implements support for connecting to multiple nodes
  and executing queries on such nodes based on a given "strategy".

  ## Usage

  To use this pool, the `:pool` option in `Xandra.start_link/1` needs to be set
  to `Xandra.Cluster`. `Xandra.Cluster` is a "proxy" pool in the sense that it
  only proxies requests to other underlying pools of Xandra connections; the
  underlying pool can be specified with the `:underlying_pool` option. When you
  start a `Xandra.Cluster` connection, it will start one pool
  (`:underlying_pool`) of connections to each of the nodes specified in
  `:nodes`. The default `:underlying_pool` is `DBConnection.Connection`, which
  means by default only a single connection to each specified node will be
  established.

  Note that regardless of the underlying pool, `Xandra.Cluster` will establish
  one extra connection to each node in the specified list of nodes (used for
  internal purposes).

  Here is an example of how one could use `Xandra.Cluster` to connect to
  multiple nodes, while using the `DBConnection.ConnectionPool` for pooling
  the connections to each node:

      Xandra.start_link([
        nodes: ["cassandra1.example.net", "cassandra2.example.net"],
        pool: Xandra.Cluster,
        underlying_pool: DBConnection.ConnectionPool,
        pool_size: 10,
      ])

  The code above will establish a pool of ten connections to each of the nodes
  specified in `:nodes`, for a total of twenty connections going out of the
  current machine, plus two extra connections (one per node) used for internal
  purposes.

  Once a `Xandra.Cluster` pool is started, queries executed through such pool
  will be "routed" to nodes in the provided list of nodes; see the "Load
  balancing strategies" section below.

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

  ## Options

  These are the options that `Xandra.start_link/1` accepts when
  `pool: Xandra.Cluster` is passed to it:

    * `:underlying_pool` - (module) the `DBConnection.Pool` pool used to pool
      connections to each of the specified nodes.

    * `:load_balancing` - (atom) load balancing "strategy". Defaults to `:random`.

  To pass options to the underlying pool, you can just pass them alongside other
  options to `Xandra.start_link/1`.
  """

  use GenServer

  @default_pool_module DBConnection.Connection
  @default_load_balancing :random
  @default_port 9042

  @default_start_options [
    nodes: ["127.0.0.1"],
    idle_interval: 30_000
  ]

  alias __MODULE__.{ControlConnection, StatusChange}
  alias Xandra.ConnectionError

  require Logger

  defstruct [
    :options,
    :node_refs,
    :load_balancing,
    :pool_supervisor,
    :pool_module,
    pools: %{}
  ]

  def start_link(options) do
    options = Keyword.merge(@default_start_options, options)

    {pool_module, options} = Keyword.pop(options, :underlying_pool, @default_pool_module)
    {load_balancing, options} = Keyword.pop(options, :load_balancing, @default_load_balancing)
    {nodes, options} = Keyword.pop(options, :nodes)
    {name, options} = Keyword.pop(options, :name)

    state = %__MODULE__{
      options: Keyword.delete(options, :pool),
      load_balancing: load_balancing,
      pool_module: pool_module
    }

    nodes = Enum.map(nodes, &parse_node/1)

    GenServer.start_link(__MODULE__, {state, nodes}, name: name)
  end

  def activate(cluster, node_ref, address, port) do
    GenServer.cast(cluster, {:activate, node_ref, address, port})
  end

  def update(cluster, status_change) do
    GenServer.cast(cluster, {:update, status_change})
  end

  def stream_pages!(conn, query, params, options \\ []) do
    Xandra.stream_pages!(conn, query, params, options)
  end

  def prepare(cluster, statement, options \\ []) when is_binary(statement) do
    with_conn(cluster, &Xandra.prepare(&1, statement, options))
  end

  def prepare!(cluster, statement, options \\ []) do
    case prepare(cluster, statement, options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  def execute(cluster, query, params_or_options \\ []) do
    with_conn(cluster, &Xandra.execute(&1, query, params_or_options))
  end

  def execute(cluster, query, params, options) do
    with_conn(cluster, &Xandra.execute(&1, query, params, options))
  end

  def execute!(cluster, query, params_or_options \\ []) do
    case execute(cluster, query, params_or_options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  def execute!(cluster, query, params, options) do
    case execute(cluster, query, params, options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  def run(cluster, options \\ [], fun) do
    with_conn(cluster, &Xandra.run(&1, options, fun))
  end

  ## Callbacks

  @impl true
  def init({%__MODULE__{options: options} = state, nodes}) do
    {:ok, pool_supervisor} = Supervisor.start_link([], strategy: :one_for_one, max_restarts: 0)
    node_refs = start_control_connections(nodes, options)
    {:ok, %{state | node_refs: node_refs, pool_supervisor: pool_supervisor}}
  end

  @impl true
  def handle_call(:checkout, _from, %__MODULE__{} = state) do
    %{
      node_refs: node_refs,
      load_balancing: load_balancing,
      pool_module: pool_module,
      pools: pools
    } = state

    if Enum.empty?(pools) do
      {:reply, {:error, :empty}, state}
    else
      pool = select_pool(load_balancing, pools, node_refs)
      {:reply, {:ok, pool_module, pool}, state}
    end
  end

  @impl true
  def handle_cast(message, state)

  def handle_cast({:activate, node_ref, address, port}, %__MODULE__{} = state) do
    {:noreply, start_pool(state, node_ref, address, port)}
  end

  def handle_cast({:update, %StatusChange{} = status_change}, %__MODULE__{} = state) do
    {:noreply, toggle_pool(state, status_change)}
  end

  ## Helpers

  defp with_conn(cluster, fun) do
    case GenServer.call(cluster, :checkout) do
      {:ok, _pool_module, pool} ->
        fun.(pool)

      {:error, :empty} ->
        action = "checkout from cluster #{inspect(cluster)}"
        {:error, ConnectionError.new(action, {:cluster, :not_connected})}
    end
  end

  defp start_control_connections(nodes, options) do
    cluster = self()

    Enum.map(nodes, fn {address, port} ->
      node_ref = make_ref()
      ControlConnection.start_link(cluster, node_ref, address, port, options)
      {node_ref, nil}
    end)
  end

  defp start_pool(state, node_ref, address, port) do
    %{
      options: options,
      node_refs: node_refs,
      pool_module: _pool_module,
      pool_supervisor: pool_supervisor,
      pools: pools
    } = state

    options = Keyword.merge(options, address: address, port: port)

    child_spec = %{
      id: address,
      start: {Xandra, :start_link, [options]},
      type: :worker
    }

    case Supervisor.start_child(pool_supervisor, child_spec) do
      {:ok, pool} ->
        node_refs = List.keystore(node_refs, node_ref, 0, {node_ref, address})
        %{state | node_refs: node_refs, pools: Map.put(pools, address, pool)}

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

  defp toggle_pool(state, %{effect: "UP", address: address}) do
    %{pool_supervisor: pool_supervisor, pools: pools} = state

    case Supervisor.restart_child(pool_supervisor, address) do
      {:error, reason} when reason in [:not_found, :running, :restarting] ->
        state

      {:ok, pool} ->
        %{state | pools: Map.put(pools, address, pool)}
    end
  end

  defp toggle_pool(state, %{effect: "DOWN", address: address}) do
    %{pool_supervisor: pool_supervisor, pools: pools} = state

    Supervisor.terminate_child(pool_supervisor, address)
    %{state | pools: Map.delete(pools, address)}
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
