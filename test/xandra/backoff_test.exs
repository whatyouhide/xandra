defmodule Xandra.BackoffTest do
  use ExUnit.Case, async: true

  alias Xandra.Backoff

  describe "new/1" do
    test "returns nil when :backoff_type is :stop" do
      assert Backoff.new(backoff_type: :stop) == nil
    end

    test "with default :backoff_min" do
      assert %Backoff{} = Backoff.new(backoff_max: 5000)
    end

    test "raises if :backoff_min is a negative integer" do
      assert_raise ArgumentError, "minimum -1 not 0 or a positive integer", fn ->
        Backoff.new(backoff_type: :exp, backoff_min: -1)
      end
    end

    test "raises if :backoff_max is a negative integer" do
      assert_raise ArgumentError, "maximum -1 not 0 or a positive integer", fn ->
        Backoff.new(backoff_type: :exp, backoff_min: 0, backoff_max: -1)
      end
    end

    test "raises if :backoff_max is smaller than :backoff_min" do
      assert_raise ArgumentError, "minimum 20 is greater than maximum 10", fn ->
        Backoff.new(backoff_type: :exp, backoff_min: 20, backoff_max: 10)
      end
    end

    test "raises for unknown type" do
      assert_raise ArgumentError, "unknown type :wat", fn ->
        Backoff.new(backoff_type: :wat)
      end
    end

    for type <- [:rand, :exp, :rand_exp] do
      test "with type #{inspect(type)}" do
        assert %Backoff{} = Backoff.new(backoff_type: unquote(type))
      end
    end
  end

  describe "backoff/1" do
    for type <- [:rand, :exp, :rand_exp] do
      test "works with type #{inspect(type)}" do
        backoff = Backoff.new(backoff_type: unquote(type))
        assert {i1, backoff} = Backoff.backoff(backoff)
        assert {i2, backoff} = Backoff.backoff(backoff)
        assert {i3, _backoff} = Backoff.backoff(backoff)

        assert is_integer(i1)
        assert is_integer(i2)
        assert is_integer(i3)
      end
    end
  end

  describe "reset/1" do
    for type <- [:rand, :exp, :rand_exp] do
      test "works with type #{inspect(type)}" do
        backoff = Backoff.new(backoff_type: unquote(type))
        assert %Backoff{} = Backoff.reset(backoff)
      end
    end
  end
end
