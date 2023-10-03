defmodule XandraTest do
  use ExUnit.Case, async: true

  import XandraTest.IntegrationCase, only: [default_start_options: 0]

  alias Xandra.ConnectionError

  doctest Xandra

  describe "options validation in Xandra.start_link/1" do
    test "with invalid nodes" do
      assert_raise NimbleOptions.ValidationError, ~r{invalid node: "foo:bar"}, fn ->
        Xandra.start_link(nodes: ["foo:bar"])
      end

      assert_raise NimbleOptions.ValidationError, ~r{invalid list in :nodes option}, fn ->
        Xandra.start_link(nodes: [:not_even_a_string])
      end

      assert_raise ArgumentError, ~r{cannot use multiple nodes in the :nodes option}, fn ->
        Xandra.start_link(nodes: ["foo", "bar"])
      end

      # Empty list of nodes.
      assert_raise ArgumentError, ~r{the :nodes option can't be an empty list}, fn ->
        Xandra.start_link(nodes: [])
      end
    end

    test "with invalid :authentication" do
      assert_raise NimbleOptions.ValidationError, ~r{invalid value for :authentication}, fn ->
        Xandra.start_link(authentication: :nope)
      end
    end

    test "with invalid :compressor" do
      message =
        "invalid value for :compressor option: compressor module :nonexisting_module is not loaded"

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.start_link(compressor: :nonexisting_module)
      end

      message = ~r/expected compressor module to be a module/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.start_link(compressor: "not even an atom")
      end
    end
  end

  test "returns an error if the connection is not established" do
    options =
      Keyword.merge(default_start_options(),
        nodes: ["nonexistent-domain"],
        queue_target: 10,
        queue_interval: 10,
        pool_size: 0
      )

    conn = start_supervised!({Xandra, options})

    assert {:error, %ConnectionError{action: "request", reason: :not_connected}} =
             Xandra.execute(conn, "USE some_keyspace")
  end
end
