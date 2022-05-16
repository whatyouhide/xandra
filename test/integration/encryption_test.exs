defmodule EncryptionTest do
  start_options = [
    nodes: ["127.0.0.1:9044"],
    encryption: true
  ]

  use XandraTest.IntegrationCase, start_options: start_options

  # TODO: unskip when we figure out how the heck to run C* 4 with encryption.
  if String.starts_with?(System.get_env("CASSANDRA_VERSION", ""), "4") do
    @moduletag :skip
  end

  @moduletag :encryption

  test "encrypted connections", %{keyspace: keyspace, start_options: start_options} do
    {:ok, conn} = Xandra.start_link(start_options)
    assert Xandra.execute!(conn, "USE #{keyspace}")
  end
end
