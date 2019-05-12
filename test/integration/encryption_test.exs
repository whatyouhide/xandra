defmodule EncryptionTest do
  use XandraTest.IntegrationCase, start_options: [ssl: true]

  @moduletag :encryption

  test "SSL connections", %{keyspace: keyspace, start_options: start_options} do
    {:ok, conn} = Xandra.start_link(start_options)
    assert Xandra.execute!(conn, "USE #{keyspace}")
  end
end
