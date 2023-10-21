defmodule EncryptionTest do
  start_options = [
    nodes: ["127.0.0.1:#{XandraTest.IntegrationCase.cassandra_port_with_ssl()}"],
    encryption: true,
    transport_options: [verify: :verify_none]
  ]

  use XandraTest.IntegrationCase, start_options: start_options, async: true

  @moduletag :encryption
  @moduletag :cassandra_specific
  @moduletag start_conn: false

  test "encrypted connections", %{keyspace: keyspace, start_options: start_options} do
    assert {:ok, conn} = start_supervised({Xandra, start_options})
    assert Xandra.execute!(conn, "USE #{keyspace}")
  end
end
