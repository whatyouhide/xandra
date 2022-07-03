defmodule CustomPayloadTest do
  use XandraTest.IntegrationCase, async: true

  @example_custom_payload %{"some_key" => <<1, 2, 3>>}

  test "sending a custom payload when executing a simple query", %{conn: conn} do
    assert {:ok, %Xandra.Page{} = response} =
             Xandra.execute(conn, "SELECT * FROM system.local", _params = [],
               custom_payload: @example_custom_payload
             )

    assert is_nil(response.custom_payload)
  end

  test "sending a custom payload when preparing a query", %{conn: conn} do
    assert {:ok, %Xandra.Prepared{} = prepared} =
             Xandra.prepare(conn, "SELECT * FROM system.local",
               custom_payload: @example_custom_payload
             )

    assert prepared.request_custom_payload == @example_custom_payload
    assert is_nil(prepared.response_custom_payload)
  end

  test "sending a custom payload when preparing a query and executing a query", %{conn: conn} do
    # Send the custom payload when preparing the query.
    assert {:ok, %Xandra.Prepared{} = prepared} =
             Xandra.prepare(conn, "SELECT * FROM system.local",
               custom_payload: @example_custom_payload
             )

    # Send the custom payload when executing the prepared query.
    assert {:ok, %Xandra.Page{} = response} =
             Xandra.execute(conn, prepared, [], custom_payload: @example_custom_payload)

    assert is_nil(response.custom_payload)
  end

  test "sending a custom payload when executing a batch query", %{conn: conn, keyspace: keyspace} do
    Xandra.execute!(conn, "CREATE TABLE #{keyspace}.users (id int, name text, PRIMARY KEY (id))")

    batch =
      Xandra.Batch.new()
      |> Xandra.Batch.add("INSERT INTO users (id, name) VALUES (1, 'Joe')")
      |> Xandra.Batch.add("INSERT INTO users (id, name) VALUES (1, 'Jane')")

    assert {:ok, %Xandra.Void{} = void_response} =
             Xandra.execute(conn, batch, custom_payload: @example_custom_payload)

    assert is_nil(void_response.custom_payload)
  end
end
