defmodule Xandra.Authenticator.Password do
  @moduledoc """
  A `Xandra.Authenticator` that implements support for PasswordAuthenticator
  to authenticate with Cassandra server.

  ## Example

    authenticator_options = [username: "xandra", password: "secret"]
    Xandra.start_link(authentication: {Xandra.Authenticator.Password, authenticator_options})

  """
  @behaviour Xandra.Authenticator

  def response_body(options) do
    [
      0x00,
      Keyword.fetch!(options, :username),
      0x00,
      Keyword.fetch!(options, :password)
    ]
  end
end
