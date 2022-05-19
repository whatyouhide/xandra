defmodule Xandra.ConnectionErrorTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Xandra.ConnectionError

  describe "message/1" do
    property "whatever the reason, it supports any binary action" do
      check all action <- string(:printable), max_runs: 10 do
        assert message(action, :econnrefused) ==
                 "action #{inspect(action)} failed with reason: connection refused"
      end
    end

    test "with reason {:unsupported_compression, algorithm}" do
      assert message("connect", {:unsupported_compression, :banana}) ==
               ~s(action "connect" failed with reason: unsupported compression algorithm :banana)
    end

    test "with reason :closed" do
      assert message("connect", :closed) ==
               ~s(action "connect" failed with reason: socket is closed)
    end

    test "with reason {:cluster, :not_connected}" do
      assert message("connect", {:cluster, :not_connected}) ==
               ~s(action "connect" failed with reason: not connected to any of the nodes)
    end

    test "with POSIX reason from :inet" do
      assert message("connect", :enomem) ==
               ~s(action "connect" failed with reason: not enough memory)

      assert message("connect", :econnrefused) ==
               ~s(action "connect" failed with reason: connection refused)
    end

    test "with any other reason" do
      assert message("connect", :banana) ==
               ~s(action "connect" failed with reason: :banana)
    end
  end

  defp message(action, reason) do
    assert %ConnectionError{} = error = ConnectionError.new(action, reason)
    ConnectionError.message(error)
  end
end
