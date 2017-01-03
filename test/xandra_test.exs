defmodule XandraTest do
  use ExUnit.Case

  test "options validation in Xandra.start_link/2" do
    message = "expected an integer as the value of the :port option, got: :not_an_integer"
    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(port: :not_an_integer)
    end
  end
end
