defmodule XandraTest do
  use ExUnit.Case, async: true

  test "options validation in Xandra.start_link/1" do
    assert_raise NimbleOptions.ValidationError, ~r{invalid node: "foo:bar"}, fn ->
      Xandra.start_link(nodes: ["foo:bar"])
    end

    assert_raise ArgumentError, ~r{cannot use multiple nodes in the :nodes option}, fn ->
      Xandra.start_link(nodes: ["foo", "bar"])
    end
  end
end
