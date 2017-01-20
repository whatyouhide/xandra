defmodule CompressionTest do
  use XandraTest.IntegrationCase, async: true

  defmodule Snappy do
    @behaviour Xandra.Compressor
    def algorithm(), do: :snappy

    def compress(binary) do
      {:ok, compressed} = :snappy.compress(binary)
      compressed
    end

    def decompress(compressed) do
      {:ok, binary} = :snappy.decompress(compressed)
      binary
    end
  end

  test "compression with the snappy algorithm", %{conn: conn, keyspace: keyspace} do
    assert {:ok, compressed_conn} = Xandra.start_link(compressor: Snappy)

    statement = "CREATE TABLE users (code int, name text, PRIMARY KEY (code, name))"
    Xandra.execute!(conn, statement, [])
    Xandra.execute!(conn, "INSERT INTO users (code, name) VALUES (1, 'Homer')")

    statement = "SELECT * FROM #{keyspace}.users WHERE code = ?"

    # We check that sending an decompressed request which will receive a
    # compressed response works.
    assert {:ok, %Xandra.Page{} = page} = Xandra.execute(compressed_conn, statement, [{"int", 1}])
    assert Enum.to_list(page) == [%{"code" => 1, "name" => "Homer"}]

    # Compressing simple queries.
    options = [compressor: Snappy]
    assert {:ok, %Xandra.Page{} = page} = Xandra.execute(compressed_conn, statement, [{"int", 1}], options)
    assert Enum.to_list(page) == [%{"code" => 1, "name" => "Homer"}]

    # Compressing preparing queries and executing prepared queries.
    options = [compressor: Snappy]
    assert {:ok, prepared} = Xandra.prepare(compressed_conn, statement, options)
    assert {:ok, %Xandra.Page{} = page} = Xandra.execute(compressed_conn, prepared, [1], options)
    assert Enum.to_list(page) == [%{"code" => 1, "name" => "Homer"}]

    # Compressing batch queries.
    options = [compressor: Snappy]
    batch =
      Xandra.Batch.new()
      |> Xandra.Batch.add("INSERT INTO #{keyspace}.users (code, name) VALUES (2, 'Marge')")
      |> Xandra.Batch.add("DELETE FROM #{keyspace}.users WHERE code = ?", [{"int", 1}])
    assert {:ok, %Xandra.Void{}} = Xandra.execute(compressed_conn, batch, options)
  end
end
