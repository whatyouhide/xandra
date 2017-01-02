defmodule Xandra.Connection.ErrorTest do
  use ExUnit.Case, async: true

  alias Xandra.Connection.Error

  test "message/1" do
    error = Error.new("connect", :econnrefused)
    assert Error.message(error) == "on action \"connect\": connection refused"
  end
end
