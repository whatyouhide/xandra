defmodule EncryptionTest do
  start_options = [
    nodes: ["127.0.0.1:9044"],
    encryption: true
  ]

  use XandraTest.IntegrationCase, start_options: start_options

  @moduletag :encryption

  # TODO: We can't quite get encryption to work on C* 4+, which is the only version that supports
  # native protocol v5 (non-beta, because C* 3 supports v5/beta). For now let's just skip
  # these tests on protocol v5, we'll work through these issues. Slow and steady!
  @moduletag skip_for_native_protocol: :v5

  test "encrypted connections", %{keyspace: keyspace, start_options: start_options} do
    {:ok, conn} = Xandra.start_link(start_options)
    assert Xandra.execute!(conn, "USE #{keyspace}")
  end
end
