defmodule GenericTest do
  use XandraTest.IntegrationCase, async: true

  test "Xandra.run/3", %{conn: conn, keyspace: keyspace} do
    assert %Xandra.SetKeyspace{} = Xandra.run(conn, [], &Xandra.execute!(&1, "USE #{keyspace}"))
  end

  test "DBConnection options in Xandra.start_link/1", %{keyspace: keyspace} do
    assert {:ok, _conn} =
             Xandra.start_link(after_connect: &Xandra.execute!(&1, "USE #{keyspace}"))
  end
end
