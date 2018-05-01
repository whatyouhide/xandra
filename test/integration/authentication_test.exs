defmodule AuthenticationTest do
  auth_options = [username: "cassandra", password: "cassandra"]
  start_options = [authentication: {Xandra.Authenticator.Password, auth_options}]

  use XandraTest.IntegrationCase, start_options: start_options

  @moduletag :authentication

  test "challenge is passed", %{keyspace: keyspace, start_options: start_options} do
    call_options = [pool: Xandra.Cluster]

    {:ok, cluster} = Xandra.start_link(call_options ++ start_options)

    assert ClusteringTest.await_connected(
             cluster,
             call_options,
             &Xandra.execute!(&1, "USE #{keyspace}")
           )
  end
end
