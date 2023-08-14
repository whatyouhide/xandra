defmodule Xandra.OptionsValidatorsTest do
  use ExUnit.Case, async: true

  alias Xandra.OptionsValidators

  describe "validate_genstatem_name/1" do
    test "with an atom" do
      assert {:ok, {:local, :foo}} = OptionsValidators.validate_genstatem_name(:foo)
    end

    test "with a {:global, _} tuple" do
      assert {:ok, {:global, :foo}} = OptionsValidators.validate_genstatem_name({:global, :foo})
    end

    test "with a {:via, _, _} tuple" do
      key = {:via, Registry, {"some", "key"}}
      assert {:ok, ^key} = OptionsValidators.validate_genstatem_name(key)
    end

    test "with an invalid term" do
      assert {:error, message} = OptionsValidators.validate_genstatem_name("whatever")

      assert message =~ "expected :name option to be one of the following"
      assert message =~ "whatever"
    end
  end
end
