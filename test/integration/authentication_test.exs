defmodule AuthenticationTest do
  auth_options = [username: "cassandra", password: "cassandra"]
  port = System.get_env("CASSANDRA_WITH_AUTH_PORT", "9053")

  use XandraTest.IntegrationCase,
    start_options: [
      authentication: {Xandra.Authenticator.Password, auth_options},
      nodes: ["127.0.0.1:#{port}"]
    ]

  alias Xandra.TestHelper

  @moduletag :authentication

  test "challenge is passed to cluster connections", %{start_options: start_options} do
    cluster = TestHelper.start_link_supervised!({Xandra.Cluster, start_options})
    TestHelper.await_cluster_connected(cluster)
  end
end
