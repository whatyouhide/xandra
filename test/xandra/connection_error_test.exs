defmodule Xandra.ConnectionErrorTest do
  use ExUnit.Case, async: true

  alias Xandra.ConnectionError

  test "message/1" do
    error = ConnectionError.new("connect", :econnrefused)

    assert ConnectionError.message(error) ==
             "action \"connect\" failed with reason: connection refused"
  end
end
