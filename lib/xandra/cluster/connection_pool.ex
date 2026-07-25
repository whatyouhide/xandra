defmodule Xandra.Cluster.ConnectionPool do
  @moduledoc false

  # A pool of Xandra connections to a single host. The pool is a supervisor with
  # two children:
  #
  #   * a ShardManager, which tracks which ScyllaDB shard each connection is on
  #     (and directs opening connections to specific shards, see the docs in
  #     that module);
  #
  #   * a plain supervisor for the connections themselves, which starts *empty*:
  #     the ShardManager populates it, so that the connection specs can point at
  #     the current ShardManager process.
  #
  # The strategy is :one_for_all: if the ShardManager crashes, the connections
  # are restarted with it, so a fresh ShardManager always (re)builds its view of
  # the connections from scratch and can never go out of sync. If connections
  # keep crashing, first the connections supervisor and then this supervisor
  # give up, and the cluster reacts by marking the host and eventually
  # restarting the whole pool.
  #
  # Checking out a connection accepts an optional partition token: when the pool
  # knows the sharding parameters of the host (ScyllaDB only), it hands out a
  # connection to the shard that owns the token, falling back to a random
  # connection otherwise.

  use Supervisor

  alias Xandra.Cluster.ConnectionPool.ShardManager

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    connection_opts = Keyword.fetch!(opts, :connection_options)
    pool_size = Keyword.fetch!(opts, :pool_size)
    shard_awareness? = Keyword.get(opts, :shard_awareness, false)
    Supervisor.start_link(__MODULE__, {connection_opts, pool_size, shard_awareness?})
  end

  # Checks out a connection from the pool. If token is an integer and the pool
  # knows the sharding parameters of the host, returns a connection to the shard
  # that owns the token (if there is one). Returns nil if the pool is shutting
  # down or has no connections.
  @spec checkout(pid(), integer() | nil) :: pid() | nil
  def checkout(sup_pid, token \\ nil) when is_pid(sup_pid) do
    case List.keyfind(Supervisor.which_children(sup_pid), ShardManager, 0) do
      {ShardManager, manager_pid, _type, _modules} when is_pid(manager_pid) ->
        ShardManager.checkout(manager_pid, token)

      _other ->
        nil
    end
  catch
    :exit, _reason -> nil
  end

  ## Callbacks

  @impl true
  def init({connection_opts, pool_size, shard_awareness?}) do
    children = [
      {ShardManager, {self(), connection_opts, pool_size, shard_awareness?}},
      %{
        id: :connections_supervisor,
        start: {Supervisor, :start_link, [[], [strategy: :one_for_one]]},
        type: :supervisor
      }
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
