defmodule AuthenticationTest do
  auth_options = [username: "cassandra", password: "cassandra"]

  start_options = [
    authentication: {Xandra.Authenticator.Password, auth_options},
    nodes: ["127.0.0.1:9043"]
  ]

  use XandraTest.IntegrationCase, start_options: start_options

  alias Xandra.TestHelper

  @moduletag :authentication

  test "challenge is passed", %{keyspace: keyspace, start_options: start_options} do
    {:ok, cluster} = Xandra.Cluster.start_link(start_options)

    assert TestHelper.await_connected(
             cluster,
             _options = [],
             &Xandra.execute!(&1, "USE #{keyspace}")
           )
  end
end
