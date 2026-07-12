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

  ## Challenge-based Authentication

  Some authenticators (such as AWS Keyspaces' `SigV4` mechanism) require an
  additional **challenge** round-trip: after the initial `AUTH_RESPONSE`, the server
  replies with an `AUTH_CHALLENGE` message that the client has to answer with another
  `AUTH_RESPONSE`. To support these authenticators, you have to implement the optional
  `c:challenge_response_body/2` callback. If it's defined, Xandra calls it with the
  challenge sent by the server whenever it receives an `AUTH_CHALLENGE` message.

      defmodule MyChallengeAuthenticator do
        @behaviour Xandra.Authenticator

        @impl true
        def response_body(_options) do
          "SigV4\\0\\0"
        end

        @impl true
        def challenge_response_body(challenge, options) do
          sign(challenge, options)
        end
      end

  """

  @doc """
  Returns an iodata that's used as the response body to Cassandra's `AUTHENTICATE` message.
  """
  @callback response_body(options :: keyword) :: iodata

  @doc """
  Returns an iodata that's used as the response body to Cassandra's `AUTH_CHALLENGE` message.

  `challenge` is the raw token sent by the server in the `AUTH_CHALLENGE` message (or `nil`
  if the server sent a null token). This callback is *optional*: implement it only for
  authenticators that use a challenge-based flow (such as AWS Keyspaces' `SigV4`). If an
  authenticator doesn't implement this callback and the server sends an `AUTH_CHALLENGE`
  message, Xandra raises an error.
  """
  @doc since: "0.20.0"
  @callback challenge_response_body(challenge :: binary() | nil, options :: keyword) :: iodata

  @optional_callbacks challenge_response_body: 2
end
