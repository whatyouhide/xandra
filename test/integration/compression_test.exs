defmodule CompressionTest do
  use XandraTest.IntegrationCase, async: true

  defmodule Snappy do
    @behaviour Xandra.Compressor

    def algorithm(), do: :snappy

    def compress(body) do
      {:ok, compressed_body} = :snappy.compress(body)
      compressed_body
    end

    def decompress(compressed_body) do
      {:ok, body} = :snappy.decompress(compressed_body)
      body
    end
  end

  setup %{conn: conn} do
    Xandra.execute!(conn, "CREATE TABLE users (code int, name text, PRIMARY KEY (code, name))")
    Xandra.execute!(conn, "INSERT INTO users (code, name) VALUES (1, 'Homer')")
    :ok
  end

  test "compression with the snappy algorithm", %{keyspace: keyspace} do
    assert {:ok, compressed_conn} = Xandra.start_link(compressor: Snappy, idle_timeout: 200)

    statement = "SELECT * FROM #{keyspace}.users WHERE code = ?"
    options = [compressor: Snappy]

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

    # This sleep is needed to test pings with compression,
    # and its value must be bigger than :idle_timeout.
    Process.sleep(250)

    # Compressing batch queries.
    batch =
      Xandra.Batch.new()
      |> Xandra.Batch.add("INSERT INTO #{keyspace}.users (code, name) VALUES (2, 'Marge')")
      |> Xandra.Batch.add("DELETE FROM #{keyspace}.users WHERE code = ?", [{"int", 1}])

    assert {:ok, %Xandra.Void{}} = Xandra.execute(compressed_conn, batch, options)
  end
end
