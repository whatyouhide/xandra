defmodule Xandra.Cluster.ConnectionPool do
  @moduledoc false

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    connection_opts = Keyword.fetch!(opts, :connection_options)
    pool_size = Keyword.fetch!(opts, :pool_size)
    Supervisor.start_link(__MODULE__, {connection_opts, pool_size})
  end

  @spec checkout(pid()) :: pid()
  def checkout(sup_pid) when is_pid(sup_pid) do
    pids =
      for {_id, pid, _type, _modules} when is_pid(pid) <- Supervisor.which_children(sup_pid) do
        pid
      end

    Enum.random(pids)
  end

  ## Callbacks

  @impl true
  def init({connection_opts, pool_size}) do
    children =
      for index <- 1..pool_size do
        Supervisor.child_spec({Xandra, connection_opts}, id: {Xandra, index})
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
