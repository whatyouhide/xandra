defmodule ProtocolNegotiationTest do
  use XandraTest.IntegrationCase, async: true

  # This test tests a few things.
  #
  #   * If we run it on C* 4, it will test that we can use native protocol v5 and it works
  #     (for when Xandra will default to trying to connect with protocol v5)
  #   * If we run it on C* 3, then C* 3 will return an error saying that v5 is behind a
  #     BETA flag (which we DO NOT support), so this test tests that Xandra can correctly
  #     downgrade to a lower version of the protocol
  #
  test "beta protocol v5", %{conn: conn} do
    assert %Xandra.Page{} = Xandra.execute!(conn, "SELECT * FROM system.local")
  end
end
