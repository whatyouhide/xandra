defmodule AuthenticationTest do
  start_options = [
    authentication: {Xandra.Authenticator.Password, [username: "cassandra", password: "cassandra"]},
  ]
  use XandraTest.IntegrationCase, start_options: start_options

  @moduletag :authentication

  test "challenge is passed", %{conn: conn, keyspace: keyspace} do
    assert Xandra.execute!(conn, "USE #{keyspace}")
  end
end
