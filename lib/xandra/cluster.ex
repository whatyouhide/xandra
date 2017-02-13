defmodule Xandra.Cluster do
  use GenServer

  @behaviour DBConnection.Pool

  @default_pool_module DBConnection.Connection

  alias __MODULE__.{ControlConnection, StatusChange, Error}

  require Logger

  defstruct [:options, :supervisor, :pool_module, pools: %{}]

  def ensure_all_started(_options, _type) do
    {:ok, []}
  end

  def child_spec(module, options, child_options) do
    Supervisor.Spec.worker(__MODULE__, [module, options], child_options)
  end

  def start_link(Xandra.Connection, options) do
    {pool_module, options} = Keyword.pop(options, :underlying_pool, @default_pool_module)
    {nodes, options} = Keyword.pop(options, :nodes)
    {name, options} = Keyword.pop(options, :name)

    state = %__MODULE__{options: Keyword.delete(options, :pool), pool_module: pool_module}
    GenServer.start_link(__MODULE__, {state, nodes}, name: name)
  end

  def init({%__MODULE__{} = state, nodes}) do
    {:ok, supervisor} = Supervisor.start_link([], strategy: :one_for_one, max_restarts: 0)
    start_control_connections(nodes)
    {:ok, %{state | supervisor: supervisor}}
  end

  def checkout(cluster, options) do
    case GenServer.call(cluster, :checkout) do
      {:ok, pool_module, pool} ->
        with {:ok, pool_ref, module, state} <- pool_module.checkout(pool, options) do
          {:ok, {pool_module, pool_ref}, module, state}
        end
      {:error, :empty} ->
        {:error, Error.exception(message: "not connected to any of the nodes")}
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

  def activate(cluster, address, port) do
    GenServer.cast(cluster, {:activate, address, port})
  end

  def update(cluster, status_change) do
    GenServer.cast(cluster, {:update, status_change})
  end

  def handle_call(:checkout, _from, %__MODULE__{pools: pools} = state) do
    if Enum.empty?(pools) do
      {:reply, {:error, :empty}, state}
    else
      {_address, pool} = Enum.random(pools)
      {:reply, {:ok, state.pool_module, pool}, state}
    end
  end

  def handle_cast({:activate, address, port}, %__MODULE__{} = state) do
    {:noreply, start_pool(state, address, port)}
  end

  def handle_cast({:update, %StatusChange{} = status_change}, %__MODULE__{} = state) do
    {:noreply, toggle_pool(state, status_change)}
  end

  defp start_control_connections(nodes) do
    cluster = self()
    Enum.each(nodes, fn({address, port}) ->
      ControlConnection.start_link(cluster, address, port)
    end)
  end

  defp start_pool(state, address, port) do
    %{pool_module: pool_module, pools: pools,
      options: options, supervisor: supervisor} = state
    options = [address: address, port: port] ++ options
    child_spec = pool_module.child_spec(Xandra.Connection, options, id: address)
    case Supervisor.start_child(supervisor, child_spec) do
      {:ok, pool} ->
        %{state | pools: Map.put(pools, address, pool)}
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
    %{pools: pools, supervisor: supervisor} = state
    case Supervisor.restart_child(supervisor, address) do
      {:error, reason} when reason in [:not_found, :running, :restarting] ->
        state
      {:ok, pool} ->
        %{state | pools: Map.put(pools, address, pool)}
    end
  end

  defp toggle_pool(state, %{effect: "DOWN", address: address}) do
    %{pools: pools, supervisor: supervisor} = state
    Supervisor.terminate_child(supervisor, address)
    %{state | pools: Map.delete(pools, address)}
  end
end
