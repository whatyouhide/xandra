defmodule Xandra.Authenticator.PasswordTest do
  use ExUnit.Case, async: true

  alias Xandra.Authenticator.Password

  describe "c:Xandra.Authenticator.response_body/1" do
    test "returns the right body with username and password" do
      body = Password.response_body(username: "my_user", password: "my_password")
      assert IO.iodata_to_binary(body) == <<0x00, "my_user", 0x00, "my_password">>
    end
  end
end
