defmodule Xandra.Cluster.ConnectionPoolTest do
  use ExUnit.Case, async: true

  alias Xandra.Cluster.ConnectionPool
  alias Xandra.Cluster.ConnectionPool.ShardManager

  # An unreachable node: connections stay alive and retry with backoff, but
  # never manage to connect.
  @unreachable_node "127.0.0.1:9"

  defp start_pool!(overrides \\ []) do
    opts =
      Keyword.merge(
        [
          connection_options: [nodes: [@unreachable_node]],
          pool_size: 1,
          shard_awareness: false
        ],
        overrides
      )

    start_supervised!({ConnectionPool, opts})
  end

  defp manager_pid(pool) do
    {ShardManager, manager_pid, _type, _modules} =
      pool |> Supervisor.which_children() |> List.keyfind(ShardManager, 0)

    manager_pid
  end

  defp tracked_connections(manager_pid) do
    # :sys.get_state/1 also acts as a synchronization barrier for the messages
    # we send to the manager in these tests.
    :sys.get_state(manager_pid).connections
  end

  defp wait_until(fun, deadline \\ System.monotonic_time(:millisecond) + 2000) do
    result = fun.()

    cond do
      result -> result
      System.monotonic_time(:millisecond) > deadline -> flunk("condition never became true")
      true -> Process.sleep(10) && wait_until(fun, deadline)
    end
  end

  test "checkout doesn't hand out connections that never connected" do
    pool = start_pool!()
    manager_pid = manager_pid(pool)

    # The pool tracks its connection, but the connection can't connect.
    assert map_size(tracked_connections(manager_pid)) == 1

    assert ConnectionPool.checkout(pool) == nil
    assert ConnectionPool.checkout(pool, _token = 0) == nil
  end

  test "checkout doesn't hand out connections that disconnected" do
    pool = start_pool!()
    manager_pid = manager_pid(pool)
    [conn_pid] = Map.keys(tracked_connections(manager_pid))
    peername = {{127, 0, 0, 1}, 9}

    # Simulate the connection reporting a successful handshake on a
    # single-shard ScyllaDB node.
    sharding_info = %{
      shard: 0,
      nr_shards: 1,
      sharding_ignore_msb: 12,
      shard_aware_port: nil,
      shard_aware_port_ssl: nil
    }

    send(manager_pid, {:xandra, :connected, peername, conn_pid})
    send(manager_pid, {:xandra, :sharding_info, conn_pid, sharding_info})
    assert %{^conn_pid => %{connected?: true, shard: 0}} = tracked_connections(manager_pid)

    # Both plain and token-based checkouts return the connection while it's
    # connected...
    assert ConnectionPool.checkout(pool) == conn_pid
    assert ConnectionPool.checkout(pool, _token = 0) == conn_pid

    # ...and stop returning it once it disconnects, even for the shard that the
    # connection was on.
    send(manager_pid, {:xandra, :disconnected, peername, conn_pid})
    assert %{^conn_pid => %{connected?: false}} = tracked_connections(manager_pid)

    assert ConnectionPool.checkout(pool) == nil
    assert ConnectionPool.checkout(pool, _token = 0) == nil
  end

  test "failures of restarted shard-targeted connections count towards giving up" do
    # A "shard-aware port" that accepts TCP connections but never completes the
    # CQL handshake, so shard-targeted connections started by the manager stay
    # quietly connecting and the test fully controls the failure messages.
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, shard_aware_port} = :inet.port(listen_socket)
    on_exit(fn -> :gen_tcp.close(listen_socket) end)

    pool = start_pool!(shard_awareness: true)
    manager_pid = manager_pid(pool)
    [base_conn_pid] = Map.keys(tracked_connections(manager_pid))
    peername = {{127, 0, 0, 1}, shard_aware_port}

    # Simulate the base connection reporting a two-shard ScyllaDB node, which
    # makes the manager start a connection targeting the missing shard 1.
    sharding_info = %{
      shard: 0,
      nr_shards: 2,
      sharding_ignore_msb: 12,
      shard_aware_port: shard_aware_port,
      shard_aware_port_ssl: nil
    }

    send(manager_pid, {:xandra, :sharding_info, base_conn_pid, sharding_info})

    connections_supervisor = connections_supervisor(pool)

    targeted_conn_pid =
      wait_until(fn -> targeted_conn_pid(connections_supervisor, _shard = 1) end)

    assert %{^targeted_conn_pid => %{target_shard: 1}} = tracked_connections(manager_pid)

    # Kill the targeted connection: its supervisor restarts it with a new PID
    # that the manager doesn't know yet.
    Process.exit(targeted_conn_pid, :kill)

    restarted_conn_pid =
      wait_until(fn ->
        case targeted_conn_pid(connections_supervisor, _shard = 1) do
          ^targeted_conn_pid -> nil
          pid -> pid
        end
      end)

    refute Map.has_key?(tracked_connections(manager_pid), restarted_conn_pid)

    # If the restarted connection can only report connection failures, those
    # must still count towards giving up on the shard-aware port.
    for _failure <- 1..3 do
      send(manager_pid, {:xandra, :failed_to_connect, peername, restarted_conn_pid})
    end

    wait_until(fn -> :sys.get_state(manager_pid).shard_aware_disabled? end)
  end

  defp connections_supervisor(pool) do
    {:connections_supervisor, connections_supervisor, _type, _modules} =
      pool |> Supervisor.which_children() |> List.keyfind(:connections_supervisor, 0)

    connections_supervisor
  end

  defp targeted_conn_pid(connections_supervisor, shard) do
    connections_supervisor
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {{:shard, ^shard}, pid, _type, _modules} when is_pid(pid) -> pid
      _other -> nil
    end)
  end
end
