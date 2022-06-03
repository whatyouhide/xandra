defmodule ProtocolNegotiationTest do
  use ExUnit.Case, async: true

  # This test tests a few things.
  #
  #   * If we run it on C* 4, it will test that we can use native protocol v5 and it works
  #     (for when Xandra will default to trying to connect with protocol v5)
  #   * If we run it on C* 3, then C* 3 will return an error saying that v5 is behind a
  #     BETA flag (which we DO NOT support), so this test tests that Xandra can correctly
  #     downgrade to a lower version of the protocol
  #
  test "beta protocol v5" do
    conn = start_supervised!({Xandra, show_sensitive_data_on_connection_error: true})
    assert %Xandra.Page{} = Xandra.execute!(conn, "SELECT * FROM system.local")
  end

  # C* 3 has no support for native protocol v5, so we can test this.
  if System.get_env("CASSANDRA_VERSION") == "3" do
    test "if a protocol version is specified, we should not gracefully downgrade" do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          {:ok, conn} =
            start_supervised(
              {Xandra, show_sensitive_data_on_connection_error: true, protocol_version: :v5}
            )

          ref = Process.monitor(conn)
          assert_receive {:DOWN, ^ref, _, _, :killed}
        end)

      assert log =~ "Beta version of the protocol used (5/v5-beta), but USE_BETA flag is unset"
    end
  end
end
