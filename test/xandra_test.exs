defmodule XandraTest do
  use ExUnit.Case, async: true

  test "options validation in Xandra.start_link/1" do
    message = "invalid item \"foo:bar\" in the :nodes option"

    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(nodes: ["foo:bar"])
    end

    message = "multi-node use requires the :pool option to be set to Xandra.Cluster"

    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(nodes: ["foo", "bar"])
    end

    message = "invalid item \"bar:baz\" in the :nodes option"

    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(nodes: ["foo", "bar:baz"], pool: Xandra.Cluster)
    end
  end
end
