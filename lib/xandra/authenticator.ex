defmodule Xandra.Authenticator do
  @moduledoc """
  A behaviour module for implementing a Cassandra authenticator

  ## Example

    defmodule MyAuthenticator do
      @behaviour Xandra.Authenticator

      def response_body(options) do
        [Keyword.fetch(options, :foo), Keyword.fetch(options, :bar)]
      end
    end

    # To use your authenticator

    Xandra.start_link(authentication: {MyAuthenticator, foo: "foo", bar: "bar"}

  Xandra supports Cassandra's PasswordAuthenticator by default, see
  `Xandra.Authenticator.Password` for more information.
  """

  @doc """
  Returns an IOdata that's used as the response body to Cassandra server's auth challenge
  """
  @callback response_body(Keyword.t) :: iodata
end
