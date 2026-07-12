defmodule Xandra.Cluster.ConnectionPool.ShardManager do
  @moduledoc false

  # The "brain" of a Xandra.Cluster.ConnectionPool. This process holds no
  # connections itself: it starts them under the pool's connections supervisor
  # (its sibling) and only *monitors* them, so that all process lifecycle
  # (restarts, shutdown ordering, and so on) stays with the supervision tree.
  #
  # Its jobs are:
  #
  #   * starting the initial :pool_size connections to the host's regular CQL
  #     port;
  #
  #   * tracking which ScyllaDB shard each connection lands on (connections
  #     report the sharding info they find in the SUPPORTED message after every
  #     successful handshake);
  #
  #   * when connected to ScyllaDB with the :shard_awareness cluster option
  #     enabled, making sure that every shard of the host has at least one
  #     connection. Missing shards are targeted deterministically by connecting
  #     to ScyllaDB's *shard-aware port*: on that port, ScyllaDB assigns the
  #     connection to the shard equal to the connection's *source port* modulo
  #     the number of shards;
  #
  #   * handing out the connection on the shard that owns a query's partition
  #     token on checkout.

  use GenServer

  alias Xandra.Cluster.Token

  # How many times a shard-targeted connection can fail to connect (for example,
  # because the shard-aware port is blocked by a firewall or NAT) before we give
  # up on shard-aware connections for this host and fall back to the plain pool.
  @max_shard_aware_connect_failures 3

  defstruct [
    :parent_supervisor,
    :connections_supervisor,
    :connection_options,
    :pool_size,
    :shard_awareness?,
    :encryption?,

    # The latest sharding info reported by any connection to this host, or nil
    # if unknown (not connected yet, or not ScyllaDB).
    # %{shard: ..., nr_shards: ..., sharding_ignore_msb: ..., shard_aware_port: ...,
    #   shard_aware_port_ssl: ...}
    :sharding_info,

    # Set to true when we give up on connecting through the shard-aware port.
    shard_aware_disabled?: false,

    # conn pid => %{shard: non_neg_integer() | nil, target_shard: non_neg_integer() | nil,
    #               connect_failures: non_neg_integer()}
    connections: %{}
  ]

  ## Public API

  @spec start_link({pid(), keyword(), pos_integer(), boolean()}) :: GenServer.on_start()
  def start_link({parent_sup, connection_opts, pool_size, shard_awareness?}) do
    GenServer.start_link(__MODULE__, {parent_sup, connection_opts, pool_size, shard_awareness?})
  end

  @spec checkout(pid(), integer() | nil) :: pid() | nil
  def checkout(manager_pid, token) when is_pid(manager_pid) do
    GenServer.call(manager_pid, {:checkout, token})
  end

  ## Callbacks

  @impl true
  def init({parent_sup, connection_opts, pool_size, shard_awareness?}) do
    state = %__MODULE__{
      parent_supervisor: parent_sup,
      connection_options: Keyword.put(connection_opts, :sharding_info_pid, self()),
      pool_size: pool_size,
      shard_awareness?: shard_awareness?,
      encryption?: Keyword.get(connection_opts, :encryption, false)
    }

    {:ok, state, {:continue, :start_initial_connections}}
  end

  @impl true
  def handle_continue(:start_initial_connections, %__MODULE__{} = state) do
    # This blocks until the parent supervisor is done starting its children,
    # which guarantees that the connections supervisor is there.
    {:connections_supervisor, connections_sup, _type, _modules} =
      state.parent_supervisor
      |> Supervisor.which_children()
      |> List.keyfind(:connections_supervisor, 0)

    state = %__MODULE__{state | connections_supervisor: connections_sup}

    state =
      Enum.reduce(1..state.pool_size, state, fn index, state_acc ->
        spec = Supervisor.child_spec({Xandra, state_acc.connection_options}, id: {Xandra, index})
        start_connection(state_acc, spec, _target_shard = nil)
      end)

    {:noreply, state}
  end

  @impl true
  def handle_call({:checkout, token}, _from, %__MODULE__{} = state) do
    {:reply, pick_connection(state, token), state}
  end

  @impl true
  def handle_info(message, state)

  def handle_info({:xandra, :sharding_info, conn_pid, sharding_info}, %__MODULE__{} = state) do
    state = register_connection_if_unknown(state, conn_pid)

    state =
      case state.connections do
        %{^conn_pid => conn_info} ->
          shard = if sharding_info, do: sharding_info.shard

          state = put_in(state.connections[conn_pid], %{conn_info | shard: shard})
          state = if sharding_info, do: %{state | sharding_info: sharding_info}, else: state

          if not is_nil(conn_info.target_shard) and shard != conn_info.target_shard do
            # A connection through the shard-aware port landed on a different
            # shard than the one we targeted: something (like NAT) is rewriting
            # source ports, so targeting shards cannot work. Give up on it to
            # avoid opening more and more connections that miss their target.
            disable_shard_awareness(state)
          else
            maybe_fill_shards(state)
          end

        _other ->
          state
      end

    {:noreply, state}
  end

  # Sent by shard-targeted connections (which have this manager as their
  # :cluster_pid): count consecutive connection failures, and give up on the
  # shard-aware port for this host if a connection keeps failing.
  def handle_info({:xandra, :failed_to_connect, _peername, conn_pid}, %__MODULE__{} = state) do
    case state.connections do
      %{^conn_pid => %{target_shard: target_shard} = conn_info}
      when not is_nil(target_shard) ->
        failures = conn_info.connect_failures + 1

        if failures >= @max_shard_aware_connect_failures do
          {:noreply, disable_shard_awareness(state)}
        else
          {:noreply, put_in(state.connections[conn_pid].connect_failures, failures)}
        end

      _other ->
        {:noreply, state}
    end
  end

  def handle_info({:xandra, :connected, _peername, conn_pid}, %__MODULE__{} = state) do
    state =
      case state.connections do
        %{^conn_pid => _conn_info} -> put_in(state.connections[conn_pid].connect_failures, 0)
        _other -> state
      end

    {:noreply, state}
  end

  def handle_info({:xandra, :disconnected, _peername, _conn_pid}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  # A connection died. Its supervisor takes care of restarting it (the new
  # process will report its sharding info once it connects), so we only need to
  # forget the old PID.
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %__MODULE__{} = state) do
    state = update_in(state.connections, &Map.delete(&1, pid))
    {:noreply, state}
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  ## Helpers

  defp start_connection(%__MODULE__{} = state, spec, target_shard) do
    case Supervisor.start_child(state.connections_supervisor, spec) do
      {:ok, conn_pid} ->
        track_connection(state, conn_pid, target_shard)

      {:error, {:already_started, conn_pid}} ->
        track_connection(state, conn_pid, target_shard)

      # The spec is there but its connection is not running: revive it.
      {:error, :already_present} ->
        case Supervisor.restart_child(state.connections_supervisor, spec.id) do
          {:ok, conn_pid} -> track_connection(state, conn_pid, target_shard)
          {:error, _reason} -> state
        end
    end
  end

  defp track_connection(%__MODULE__{} = state, conn_pid, target_shard) do
    if Map.has_key?(state.connections, conn_pid) do
      state
    else
      Process.monitor(conn_pid)

      put_in(
        state.connections[conn_pid],
        %{shard: nil, target_shard: target_shard, connect_failures: 0}
      )
    end
  end

  # Registers a connection that we don't know yet, which happens when a
  # connection crashes and its supervisor restarts it: the new process reports
  # its sharding info, but we only know the old PID. The connection's child ID
  # in the supervisor tells us whether it targets a specific shard.
  defp register_connection_if_unknown(%__MODULE__{} = state, conn_pid) do
    if Map.has_key?(state.connections, conn_pid) do
      state
    else
      child_id =
        state.connections_supervisor
        |> Supervisor.which_children()
        |> Enum.find_value(fn {id, pid, _type, _modules} -> if pid == conn_pid, do: id end)

      case child_id do
        {Xandra, _index} -> track_connection(state, conn_pid, _target_shard = nil)
        {:shard, target_shard} -> track_connection(state, conn_pid, target_shard)
        # Not one of our connections (or already gone): ignore it.
        nil -> state
      end
    end
  end

  defp maybe_fill_shards(%__MODULE__{shard_awareness?: false} = state), do: state
  defp maybe_fill_shards(%__MODULE__{shard_aware_disabled?: true} = state), do: state
  defp maybe_fill_shards(%__MODULE__{sharding_info: nil} = state), do: state

  defp maybe_fill_shards(%__MODULE__{sharding_info: %{nr_shards: nr_shards}} = state) do
    if shard_aware_port(state) do
      covered_shards =
        for {_pid, %{shard: shard, target_shard: target_shard}} <- state.connections,
            covered = shard || target_shard,
            into: MapSet.new(),
            do: covered

      Enum.reduce(0..(nr_shards - 1), state, fn shard, state_acc ->
        if MapSet.member?(covered_shards, shard) do
          state_acc
        else
          start_connection(state_acc, shard_targeted_spec(state_acc, shard), shard)
        end
      end)
    else
      # Old ScyllaDB without a shard-aware port: we cannot target shards.
      disable_shard_awareness(state)
    end
  end

  defp shard_targeted_spec(%__MODULE__{} = state, shard) do
    # Shard-targeted connections report to this manager instead of the cluster:
    # their failures don't mean that the host is down, and their peername
    # (host + shard-aware port) is unknown to the cluster.
    options =
      state.connection_options
      |> Keyword.put(:cluster_pid, self())
      |> Keyword.put(:shard_target, %{
        shard: shard,
        nr_shards: state.sharding_info.nr_shards,
        port: shard_aware_port(state)
      })

    Supervisor.child_spec({Xandra, options}, id: {:shard, shard})
  end

  defp disable_shard_awareness(%__MODULE__{} = state) do
    state = %{state | shard_aware_disabled?: true}

    # Terminate and remove all shard-targeted connections through the
    # supervisor, so that shutdown semantics stay in supervision land.
    for {child_id = {:shard, _shard}, _pid, _type, _modules} <-
          Supervisor.which_children(state.connections_supervisor) do
      _ = Supervisor.terminate_child(state.connections_supervisor, child_id)
      _ = Supervisor.delete_child(state.connections_supervisor, child_id)
    end

    # The monitors of the terminated connections will fire and clean these up
    # anyway, but let's not hold on to dead PIDs in the meantime.
    update_in(state.connections, fn connections ->
      for {pid, conn_info} <- connections,
          is_nil(conn_info.target_shard),
          into: %{},
          do: {pid, conn_info}
    end)
  end

  defp shard_aware_port(%__MODULE__{sharding_info: sharding_info} = state) do
    if state.encryption? do
      sharding_info.shard_aware_port_ssl
    else
      sharding_info.shard_aware_port
    end
  end

  defp pick_connection(%__MODULE__{connections: connections}, _token)
       when map_size(connections) == 0 do
    nil
  end

  defp pick_connection(%__MODULE__{} = state, token) do
    with true <- is_integer(token),
         %{nr_shards: nr_shards, sharding_ignore_msb: ignore_msb} <- state.sharding_info,
         shard = Token.shard(token, nr_shards, ignore_msb),
         [_ | _] = candidates <-
           for({pid, %{shard: ^shard}} <- state.connections, do: pid) do
      Enum.random(candidates)
    else
      _fallback -> Enum.random(Map.keys(state.connections))
    end
  end
end
