# This is in a separate test file so that we can run the rest of the tests with async: true.
defmodule XandraToxiproxyTest do
  use XandraTest.IntegrationCase, async: false

  alias Xandra.ConnectionError

  @moduletag :toxiproxy

  @toxic_proxy_node "localhost:19052"

  setup %{start_options: opts} do
    %{start_options: Keyword.merge(opts, nodes: [@toxic_proxy_node])}
  end

  test "execute/3,4 supports a network that slices packets",
       %{start_options: opts, keyspace: keyspace} do
    ToxiproxyEx.get!(:xandra_test_cassandra_sliced)
    |> ToxiproxyEx.toxic(:slicer, average_size: 50, size_variation: 25, delay: _microsec = 50)
    |> ToxiproxyEx.apply!(fn ->
      opts = Keyword.merge(opts, keyspace: keyspace)
      conn = start_supervised!({Xandra, opts})
      assert {:ok, prepared} = Xandra.prepare(conn, "SELECT * FROM system.local WHERE key = ?")
      assert {:ok, page} = Xandra.execute(conn, prepared, ["local"])
      assert [%{}] = Enum.to_list(page)
    end)
  end

  test "prepare/3 when the connection is down", %{start_options: opts} do
    conn = start_supervised!({Xandra, opts})

    ToxiproxyEx.get!(:xandra_test_cassandra)
    |> ToxiproxyEx.down!(fn ->
      assert {:error, %ConnectionError{reason: :not_connected}} =
               Xandra.prepare(conn, "SELECT * FROM system.local")
    end)
  end

  test "prepare/3 when the connection goes down mid request", %{start_options: opts} do
    conn = start_supervised!({Xandra, opts})

    ToxiproxyEx.get!(:xandra_test_cassandra)
    |> ToxiproxyEx.toxic(:limit_data, bytes: 100)
    |> ToxiproxyEx.apply!(fn ->
      assert {:error, %ConnectionError{reason: :disconnected}} =
               Xandra.prepare(conn, "SELECT * FROM system.local")
    end)
  end

  test "start_link/1 supports the :connect_timeout option", %{start_options: opts} do
    opts = Keyword.merge(opts, connect_timeout: 0, backoff_type: :stop)

    ToxiproxyEx.get!(:xandra_test_cassandra)
    |> ToxiproxyEx.toxic(:timeout, timeout: 0)
    |> ToxiproxyEx.apply!(fn ->
      assert {:ok, conn} = start_supervised({Xandra, opts})

      ref = Process.monitor(conn)
      assert_receive {:DOWN, ^ref, _, _, reason}
      assert reason == :timeout
    end)
  end
end
