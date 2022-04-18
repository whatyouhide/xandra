defmodule Xandra.Cluster.NodeSupervisor do
  @moduledoc false

  use Supervisor

  @spec start_link(Supervisor.child_spec()) :: Supervisor.on_start()
  def start_link(control_conn_spec) do
    Supervisor.start_link(__MODULE__, control_conn_spec)
  end

  @impl true
  def init(control_conn_spec) do
    Supervisor.init([control_conn_spec], strategy: :one_for_one)
  end
end
