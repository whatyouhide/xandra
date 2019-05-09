defmodule XandraTest do
  use ExUnit.Case, async: true

  test "options validation in Xandra.start_link/1" do
    message = "invalid item \"foo:bar\" in the :nodes option"

    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(nodes: ["foo:bar"])
    end

    message = "multi-node use requires Xandra.Cluster instead of Xandra"

    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(nodes: ["foo", "bar"])
    end

    message = "invalid item \"bar:baz\" in the :nodes option"

    assert_raise ArgumentError, message, fn ->
      Xandra.start_link(nodes: ["bar:baz"])
    end
  end
end
