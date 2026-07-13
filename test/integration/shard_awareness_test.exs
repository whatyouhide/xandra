defmodule ShardAwarenessTest do
  # This test needs the multi-shard ScyllaDB instance (the scylladb_shard_aware
  # service in docker-compose.yml), which listens on port 9072 and exposes the
  # shard-aware port 19042.
  use ExUnit.Case

  alias Xandra.Cluster.Token

  @moduletag :scylla_specific

  @port String.to_integer(System.get_env("SCYLLA_SHARD_AWARE_CQL_PORT", "9072"))

  setup do
    protocol_version = XandraTest.IntegrationCase.protocol_version()

    options = [
      nodes: ["127.0.0.1:#{@port}"],
      shard_awareness: true,
      token_aware_routing: true,
      sync_connect: 5000
    ]

    options =
      if protocol_version do
        Keyword.put(options, :protocol_version, protocol_version)
      else
        options
      end

    cluster = start_supervised!({Xandra.Cluster, options})
    %{cluster: cluster, protocol_version: protocol_version}
  end

  test "discovers ScyllaDB sharding info and covers all shards (or cleanly falls back)",
       %{cluster: cluster} do
    pool_state = wait_for_sharding(cluster)

    # The SUPPORTED options of the multi-shard ScyllaDB instance were parsed.
    assert %{nr_shards: nr_shards, sharding_ignore_msb: _, shard_aware_port: 19_042} =
             pool_state.sharding_info

    assert nr_shards >= 2

    cond do
      pool_state.shard_aware_disabled? ->
        # The environment rewrites source ports (NAT, Docker userland proxy) or
        # blocks the shard-aware port, so Xandra correctly gave up. The plain
        # pool must still work.
        assert map_size(pool_state.connections) >= 1

      true ->
        covered_shards =
          pool_state.connections
          |> Enum.map(fn {_pid, %{shard: shard}} -> shard end)
          |> Enum.reject(&is_nil/1)
          |> Enum.uniq()
          |> Enum.sort()

        assert covered_shards == Enum.to_list(0..(nr_shards - 1))

        # Checking out a connection for a token returns a connection on the
        # shard that owns that token.
        for token <- [0, -1, 12_345_678_901, -9_223_372_036_854_775_808] do
          expected_shard =
            Token.shard(token, nr_shards, pool_state.sharding_info.sharding_ignore_msb)

          conn = Xandra.Cluster.ConnectionPool.checkout(pool_pid(cluster), token)
          assert pool_state.connections[conn].shard == expected_shard
        end
    end
  end

  test "routes prepared queries by token end-to-end", %{
    cluster: cluster,
    protocol_version: protocol_version
  } do
    _pool_state = wait_for_sharding(cluster)

    Xandra.Cluster.execute!(
      cluster,
      """
      CREATE KEYSPACE IF NOT EXISTS xandra_test_shard_awareness
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
      """,
      []
    )

    Xandra.Cluster.execute!(
      cluster,
      "CREATE TABLE IF NOT EXISTS xandra_test_shard_awareness.items (id int PRIMARY KEY, name text)",
      []
    )

    insert =
      Xandra.Cluster.prepare!(
        cluster,
        "INSERT INTO xandra_test_shard_awareness.items (id, name) VALUES (?, ?)"
      )

    select =
      Xandra.Cluster.prepare!(
        cluster,
        "SELECT name FROM xandra_test_shard_awareness.items WHERE id = ?"
      )

    # Protocol v4+ returns partition key indices for fully-bound partition
    # keys; protocol v3 doesn't support them, so queries fall back to the
    # load-balancing policy (but must still work).
    if protocol_version == :v3 do
      assert insert.pk_indices == nil
      assert select.pk_indices == nil
    else
      assert insert.pk_indices == [0]
      assert select.pk_indices == [0]
    end

    for id <- 1..50 do
      Xandra.Cluster.execute!(cluster, insert, [id, "name-#{id}"])
    end

    for id <- 1..50 do
      assert [%{"name" => name}] =
               cluster |> Xandra.Cluster.execute!(select, [id]) |> Enum.to_list()

      assert name == "name-#{id}"
    end
  end

  defp pool_pid(cluster) do
    {_state, data} = :sys.get_state(cluster)
    [%{pool_pid: pool_pid}] = Map.values(data.peers)
    pool_pid
  end

  # Digs the ShardManager state out of the pool supervisor.
  defp shard_manager_state(cluster) do
    {Xandra.Cluster.ConnectionPool.ShardManager, manager_pid, _type, _modules} =
      cluster
      |> pool_pid()
      |> Supervisor.which_children()
      |> List.keyfind(Xandra.Cluster.ConnectionPool.ShardManager, 0)

    :sys.get_state(manager_pid)
  end

  # Waits for the connection pool to discover the sharding info of the host
  # and to either cover all shards or give up on shard awareness.
  defp wait_for_sharding(cluster, deadline \\ System.monotonic_time(:millisecond) + 10_000) do
    pool_state = shard_manager_state(cluster)

    all_shards_covered? =
      match?(%{nr_shards: _}, pool_state.sharding_info) and
        pool_state.sharding_info.nr_shards ==
          pool_state.connections
          |> Enum.map(fn {_pid, %{shard: shard}} -> shard end)
          |> Enum.reject(&is_nil/1)
          |> Enum.uniq()
          |> length()

    cond do
      all_shards_covered? or pool_state.shard_aware_disabled? ->
        pool_state

      System.monotonic_time(:millisecond) > deadline ->
        flunk("""
        sharding info was not fully discovered in time. Pool state:
        #{inspect(pool_state)}
        """)

      true ->
        Process.sleep(100)
        wait_for_sharding(cluster, deadline)
    end
  end
end
