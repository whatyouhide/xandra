defmodule ScyllaSpecificTest do
  use XandraTest.IntegrationCase, async: true

  @moduletag :scylla_specific

  test "using BYPASS CACHE", %{conn: conn} do
    query = """
    SELECT * FROM system.local
    BYPASS CACHE
    """

    assert {:ok, page} = Xandra.execute(conn, query)
    assert [%{"key" => "local"}] = Enum.to_list(page)
  end
end
