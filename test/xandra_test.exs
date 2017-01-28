defmodule XandraTest do
  use ExUnit.Case, async: true

  test "options validation in Xandra.start_link/1" do
    message = "invalid item \"foo:bar\" in the :nodes option"
    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(nodes: ["foo:bar"])
    end

    message = "multi-node usage requires the :pool option set to Xandra.Cluster"
    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(nodes: ["foo", "bar"])
    end
  end
end
