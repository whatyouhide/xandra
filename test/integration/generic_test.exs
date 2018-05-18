defmodule GenericTest do
  use XandraTest.IntegrationCase, async: true

  test "Xandra.run/3", %{conn: conn, keyspace: keyspace} do
    assert %Xandra.SetKeyspace{} = Xandra.run(conn, [], &Xandra.execute!(&1, "USE #{keyspace}"))
  end
end
