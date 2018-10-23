defmodule Xandra.Cluster do
  @moduledoc """
  A `DBConnection.Pool` pool that implements clustering support.

  This module implements a `DBConnection.Pool` pool that implements support for
  connecting to multiple nodes and executing queries on such nodes based on a
  given "strategy".

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
  multiple nodes, while using `:poolboy` for pooling the connections to each
  node:

      Xandra.start_link([
        nodes: ["cassandra1.example.net", "cassandra2.example.net"],
        pool: Xandra.Cluster,
        underlying_pool: DBConnection.Poolboy,
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

  @behaviour DBConnection.Pool

  @default_pool_module DBConnection.Connection
  @default_load_balancing :random

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

  def ensure_all_started(options, type) do
    {pool_module, options} = Keyword.pop(options, :underlying_pool, @default_pool_module)
    pool_module.ensure_all_started(options, type)
  end

  def child_spec(module, options, child_options) do
    Supervisor.Spec.worker(__MODULE__, [module, options], child_options)
  end

  def start_link(Xandra.Connection, options) do
    {pool_module, options} = Keyword.pop(options, :underlying_pool, @default_pool_module)
    {load_balancing, options} = Keyword.pop(options, :load_balancing, @default_load_balancing)
    {nodes, options} = Keyword.pop(options, :nodes)
    {name, options} = Keyword.pop(options, :name)

    state = %__MODULE__{
      options: Keyword.delete(options, :pool),
      load_balancing: load_balancing,
      pool_module: pool_module
    }

    GenServer.start_link(__MODULE__, {state, nodes}, name: name)
  end

  def init({%__MODULE__{options: options} = state, nodes}) do
    {:ok, pool_supervisor} = Supervisor.start_link([], strategy: :one_for_one, max_restarts: 0)
    node_refs = start_control_connections(nodes, options)
    {:ok, %{state | node_refs: node_refs, pool_supervisor: pool_supervisor}}
  end

  def checkout(cluster, options) do
    case GenServer.call(cluster, :checkout) do
      {:ok, pool_module, pool} ->
        with {:ok, pool_ref, module, state} <- pool_module.checkout(pool, options) do
          {:ok, {pool_module, pool_ref}, module, state}
        end

      {:error, :empty} ->
        action = "checkout from cluster #{inspect(name())}"
        {:error, ConnectionError.new(action, {:cluster, :not_connected})}
    end
  end

  def checkin({pool_module, pool_ref}, state, options) do
    pool_module.checkin(pool_ref, state, options)
  end

  def disconnect({pool_module, pool_ref}, error, state, options) do
    pool_module.disconnect(pool_ref, error, state, options)
  end

  def stop({pool_module, pool_ref}, error, state, options) do
    pool_module.stop(pool_ref, error, state, options)
  end

  def activate(cluster, node_ref, address, port) do
    GenServer.cast(cluster, {:activate, node_ref, address, port})
  end

  def update(cluster, status_change) do
    GenServer.cast(cluster, {:update, status_change})
  end

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

  def handle_cast({:activate, node_ref, address, port}, %__MODULE__{} = state) do
    {:noreply, start_pool(state, node_ref, address, port)}
  end

  def handle_cast({:update, %StatusChange{} = status_change}, %__MODULE__{} = state) do
    {:noreply, toggle_pool(state, status_change)}
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
      pool_module: pool_module,
      pool_supervisor: pool_supervisor,
      pools: pools
    } = state

    options = [address: address, port: port] ++ options
    child_spec = pool_module.child_spec(Xandra.Connection, options, id: address)

    case Supervisor.start_child(pool_supervisor, child_spec) do
      {:ok, pool} ->
        node_refs = List.keystore(node_refs, node_ref, 0, {node_ref, address})
        %{state | node_refs: node_refs, pools: Map.put(pools, address, pool)}

      {:error, {:already_started, _pool}} ->
        Logger.warn(fn ->
          "Xandra cluster #{inspect(name())} " <>
            "received request to start another connection pool " <>
            "to the same address: #{inspect(address)}"
        end)

        state
    end
  end

  defp name() do
    case Process.info(self(), :registered_name) |> elem(1) do
      [] -> self()
      name -> name
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
end
