defmodule AuthenticationTest do
  use XandraTest.IntegrationCase,
    start_options: [
      authentication:
        {Xandra.Authenticator.Password, [username: "cassandra", password: "cassandra"]},
      nodes: ["127.0.0.1:#{XandraTest.IntegrationCase.port_with_auth()}"]
    ],
    async: true

  alias Xandra.TestHelper

  @moduletag :authentication

  @tag start_conn: false
  test "challenge is passed to cluster connections", %{start_options: start_options} do
    cluster = start_link_supervised!({Xandra.Cluster, start_options})
    TestHelper.await_cluster_connected(cluster)
  end
end
