defmodule Xandra.OptionsValidatorsTest do
  use ExUnit.Case, async: true

  alias Xandra.OptionsValidators

  describe "validate_node/1" do
    test "parses unbracketed IPv6 addresses without a port" do
      assert OptionsValidators.validate_node("::1") == {:ok, {"::1", 9042}}

      assert OptionsValidators.validate_node("2001:db8::1:9042") ==
               {:ok, {"2001:db8::1:9042", 9042}}
    end

    test "requires brackets around IPv6 addresses with an explicit port" do
      assert OptionsValidators.validate_node("[::1]:9043") == {:ok, {"::1", 9043}}
      assert OptionsValidators.validate_node("[::1]") == {:ok, {"::1", 9042}}
    end

    test "rejects malformed bracket notation" do
      assert {:error, _message} = OptionsValidators.validate_node("[::1:9043")
      assert {:error, _message} = OptionsValidators.validate_node("::1]:9043")
      assert {:error, _message} = OptionsValidators.validate_node("[example.com]:9043")
    end

    test "parses hostnames and IPv4 addresses as before" do
      assert OptionsValidators.validate_node("localhost") == {:ok, {"localhost", 9042}}
      assert OptionsValidators.validate_node("localhost:9043") == {:ok, {"localhost", 9043}}
      assert OptionsValidators.validate_node("127.0.0.1:9043") == {:ok, {"127.0.0.1", 9043}}
    end
  end
end
