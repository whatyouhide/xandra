defmodule Xandra.Authenticator.Password do
  @moduledoc """
  A `Xandra.Authenticator` that implements support for PasswordAuthenticator
  to authenticate with Cassandra server.

  ## Example

      options = [username: "xandra", password: "secret"]

      Xandra.start_link(authentication: {Xandra.Authenticator.Password, options})

  """
  @behaviour Xandra.Authenticator

  @impl true
  def response_body(options) when is_list(options) do
    [
      0x00,
      Keyword.fetch!(options, :username),
      0x00,
      Keyword.fetch!(options, :password)
    ]
  end
end
