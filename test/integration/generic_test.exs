defmodule GenericTest do
  use XandraTest.IntegrationCase, async: true

  test "Xandra.run/3", %{conn: conn, keyspace: keyspace} do
    assert {:ok, _} = Xandra.run(conn, [], fn conn -> Xandra.execute(conn, "USE #{keyspace}", []) end)
  end
end
