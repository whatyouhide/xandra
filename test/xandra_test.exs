defmodule XandraTest do
  use ExUnit.Case, async: true

  test "options validation in Xandra.start_link/1" do
    message = "expected an integer as the value of the :port option, got: :invalid"
    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(port: :invalid)
    end

    message = "expected a string as the value of the :host option, got: :invalid"
    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(host: :invalid)
    end
  end
end
