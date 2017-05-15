defmodule Xandra.Authenticator.Password do
  @behaviour Xandra.Authenticator

  def response_body(options) do
    [
      0x00,
      Keyword.fetch!(options, :username),
      0x00,
      Keyword.fetch!(options, :password),
    ]
  end
end
