defmodule CompressionTest do
  use XandraTest.IntegrationCase, async: true

  @moduletag skip_for_native_protocol: :v5

  defmodule LZ4 do
    @behaviour Xandra.Compressor

    @impl true
    def algorithm(), do: :lz4

    @impl true
    def compress(body) do
      # 32-bit big-endian integer with the size of the uncompressed body followed by
      # the compressed body.
      [<<IO.iodata_length(body)::4-unit(8)-integer>>, NimbleLZ4.compress(body)]
    end

    @impl true
    def decompress(<<uncompressed_size::4-unit(8)-integer, compressed_body::binary>>) do
      {:ok, body} = NimbleLZ4.decompress(compressed_body, uncompressed_size)
      body
    end
  end

  setup %{conn: conn} do
    Xandra.execute!(conn, "CREATE TABLE users (code int, name text, PRIMARY KEY (code, name))")
    Xandra.execute!(conn, "INSERT INTO users (code, name) VALUES (1, 'Homer')")
    :ok
  end

  test "compression with the lz4 algorithm", %{
    keyspace: keyspace,
    start_options: start_options
  } do
    assert {:ok, compressed_conn} =
             Xandra.start_link(start_options ++ [compressor: LZ4, idle_interval: 200])

    statement = "SELECT * FROM #{keyspace}.users WHERE code = ?"
    options = [compressor: LZ4]

    # We check that sending a non-compressed request which will receive a
    # compressed response works.
    assert {:ok, %Xandra.Page{} = page} = Xandra.execute(compressed_conn, statement, [{"int", 1}])
    assert Enum.to_list(page) == [%{"code" => 1, "name" => "Homer"}]

    # Compressing simple queries.
    assert {:ok, %Xandra.Page{} = page} =
             Xandra.execute(compressed_conn, statement, [{"int", 1}], options)

    assert Enum.to_list(page) == [%{"code" => 1, "name" => "Homer"}]

    # Compressing preparing queries and executing prepared queries.
    assert {:ok, prepared} = Xandra.prepare(compressed_conn, statement, options)
    assert {:ok, %Xandra.Page{} = page} = Xandra.execute(compressed_conn, prepared, [1], options)
    assert Enum.to_list(page) == [%{"code" => 1, "name" => "Homer"}]

    # This sleep is needed to test pings with compression, and its value must
    # be bigger than :idle_interval.
    Process.sleep(250)

    # Compressing batch queries.
    batch =
      Xandra.Batch.new()
      |> Xandra.Batch.add("INSERT INTO #{keyspace}.users (code, name) VALUES (2, 'Marge')")
      |> Xandra.Batch.add("DELETE FROM #{keyspace}.users WHERE code = ?", [{"int", 1}])

    assert {:ok, %Xandra.Void{}} = Xandra.execute(compressed_conn, batch, options)
  end
end
