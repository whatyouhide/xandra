defmodule Xandra.Authenticator do
  @moduledoc """
  A behaviour module for implementing a Cassandra authenticator.

  ## Examples

      defmodule MyAuthenticator do
        @behaviour Xandra.Authenticator

        def response_body(options) do
          ["user:", Keyword.fetch!(options, :user), "_password:", Keyword.fetch!(options, :password)]
        end
      end

  To use the authenticator defined above:

      Xandra.start_link(authentication: {MyAuthenticator, user: "foo", password: "bar"})

  Xandra supports Cassandra's PasswordAuthenticator by default, see
  `Xandra.Authenticator.Password` for more information.
  """

  @doc """
  Returns an iodata that's used as the response body to Cassandra's auth challenge.
  """
  @callback response_body(options :: Keyword.t()) :: iodata
end
