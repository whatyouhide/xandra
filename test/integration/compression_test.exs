defmodule CompressionTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.TestHelper.LZ4Compressor

  @moduletag :compression

  setup_all %{keyspace: keyspace, setup_conn: conn} do
    Xandra.execute!(
      conn,
      "CREATE TABLE #{keyspace}.users (code int, name text, PRIMARY KEY (code, name))"
    )

    Xandra.execute!(conn, "INSERT INTO #{keyspace}.users (code, name) VALUES (1, 'Homer')")

    :ok
  end

  describe "compression with the LZ4 algorithm" do
    setup %{start_options: start_options} do
      %{start_options: start_options ++ [compressor: LZ4Compressor]}
    end

    test "with simple queries", %{keyspace: keyspace, start_options: start_options} do
      compressed_conn = start_supervised!({Xandra, start_options})

      for options <- [[], [compressor: LZ4Compressor]] do
        statement = "SELECT * FROM #{keyspace}.users WHERE code = ?"

        assert {:ok, %Xandra.Page{} = page} =
                 Xandra.execute(compressed_conn, statement, [{"int", 1}], options)

        assert Enum.to_list(page) == [%{"code" => 1, "name" => "Homer"}]
      end
    end

    test "with prepared queries", %{keyspace: keyspace, start_options: start_options} do
      compressed_conn = start_supervised!({Xandra, start_options})

      for options <- [[], [compressor: LZ4Compressor]] do
        statement = "SELECT * FROM #{keyspace}.users WHERE code = ?"
        assert {:ok, prepared} = Xandra.prepare(compressed_conn, statement, options)

        assert {:ok, %Xandra.Page{} = page} =
                 Xandra.execute(compressed_conn, prepared, [1], options)

        assert Enum.to_list(page) == [%{"code" => 1, "name" => "Homer"}]
      end
    end

    test "with batch queries", %{keyspace: keyspace, start_options: start_options} do
      compressed_conn = start_supervised!({Xandra, start_options})

      for options <- [[], [compressor: LZ4Compressor]] do
        batch =
          Xandra.Batch.new()
          |> Xandra.Batch.add("INSERT INTO #{keyspace}.users (code, name) VALUES (2, 'Marge')")
          |> Xandra.Batch.add("DELETE FROM #{keyspace}.users WHERE code = ?", [{"int", 2}])

        assert {:ok, %Xandra.Void{}} = Xandra.execute(compressed_conn, batch, options)
      end
    end
  end
end
