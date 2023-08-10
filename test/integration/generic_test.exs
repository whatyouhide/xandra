defmodule GenericTest do
  use XandraTest.IntegrationCase, async: true

  test "Xandra.run/3", %{conn: conn, keyspace: keyspace} do
    assert %Xandra.SetKeyspace{} = Xandra.run(conn, [], &Xandra.execute!(&1, "USE #{keyspace}"))
  end

  test "DBConnection options in Xandra.start_link/1", %{
    conn: conn,
    keyspace: keyspace,
    start_options: start_options
  } do
    Xandra.execute!(conn, "CREATE TABLE users (code int, name text, PRIMARY KEY (code, name))")
    Xandra.execute!(conn, "INSERT INTO users (code, name) VALUES (1, 'Meg')")

    assert {:ok, test_conn} =
             start_supervised(
               {Xandra, [after_connect: &Xandra.execute(&1, "USE #{keyspace}")] ++ start_options}
             )

    assert test_conn |> Xandra.execute!("SELECT * FROM users") |> Enum.to_list() == [
             %{"code" => 1, "name" => "Meg"}
           ]
  end

  # Regression for https://github.com/lexhide/xandra/issues/266
  # This test doesn't test much for now, it's sort of a smoke test. Once we'll have
  # generic telemetry events for queries, we can change this test to assert on the telemetry
  # event.
  test ":default_consistency option when starting", %{
    keyspace: keyspace,
    start_options: start_options
  } do
    assert {:ok, test_conn} =
             start_supervised({Xandra, [default_consistency: :three] ++ start_options})

    Xandra.execute!(test_conn, "USE #{keyspace}")
  end
end
