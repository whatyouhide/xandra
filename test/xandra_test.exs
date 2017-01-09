defmodule XandraTest do
  use ExUnit.Case, async: true

  test "options validation in Xandra.start_link/1" do
    message = "expected an integer as the value of the :port option, got: :not_an_integer"
    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(port: :not_an_integer)
    end

    message = "expected a string as the value of the :host option, got: :not_a_string"
    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(host: :not_a_string)
    end
  end
end
