defmodule XandraTest do
  use ExUnit.Case, async: true

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

  test "supports DBConnection.status/1 without raising" do
    conn = start_supervised!(Xandra)
    assert DBConnection.status(conn) == :idle
  end

  test "raises for unsupported DBConnection callbacks" do
    conn = start_supervised!(Xandra)

    assert_raise ArgumentError, "Cassandra doesn't support transactions", fn ->
      assert DBConnection.transaction(conn, fn _ -> :ok end)
    end
  end

  @tag :capture_log
  test "rescues DBConnection errors" do
    conn =
      start_supervised!(
        {Xandra,
         nodes: ["nonexistent-domain"], queue_target: 10, queue_interval: 10, pool_size: 0}
      )

    assert {:error, %DBConnection.ConnectionError{}} = Xandra.execute(conn, "USE some_keyspace")
  end
end
