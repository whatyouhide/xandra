defmodule GenericTest do
  use XandraTest.IntegrationCase, async: true

  test "Xandra.run/3", %{conn: conn, keyspace: keyspace} do
    assert %Xandra.SetKeyspace{} = Xandra.run(conn, [], &Xandra.execute!(&1, "USE #{keyspace}"))
  end

  test "DBConnection options in Xandra.start_link/1", %{conn: conn, keyspace: keyspace} do
    Xandra.execute!(conn, "CREATE TABLE users (code int, name text, PRIMARY KEY (code, name))")
    Xandra.execute!(conn, "INSERT INTO users (code, name) VALUES (1, 'Meg')")

    assert {:ok, test_conn} =
             start_supervised({Xandra, after_connect: &Xandra.execute(&1, "USE #{keyspace}")})

    assert test_conn |> Xandra.execute!("SELECT * FROM users") |> Enum.to_list() == [
             %{"code" => 1, "name" => "Meg"}
           ]
  end
end
