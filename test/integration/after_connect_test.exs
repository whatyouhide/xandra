defmodule AfterConnectTest do
  use XandraTest.IntegrationCase, async: true

  test "with an anonymous function setting the keyspace", %{
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

    # TODO: after_connect doesn't block other requests for now.
    Process.sleep(500)

    assert test_conn |> Xandra.execute!("SELECT * FROM users") |> Enum.to_list() == [
             %{"code" => 1, "name" => "Meg"}
           ]
  end

  test "with a MFA that just mirrors a message", %{start_options: start_options} do
    test_pid = self()
    ref = make_ref()

    assert {:ok, test_conn} =
             start_supervised(
               {Xandra,
                [after_connect: {__MODULE__, :__send_back__, [test_pid, ref]}] ++ start_options}
             )

    assert_receive {^ref, ^test_conn}
  end

  @tag :capture_log
  test "with a function that crashes", %{start_options: start_options} do
    after_connect = fn _conn -> raise "oops" end

    assert {:ok, _test_conn} =
             start_supervised({Xandra, [after_connect: after_connect] ++ start_options})
  end

  def __send_back__(conn, test_pid, ref) do
    send(test_pid, {ref, conn})
  end
end
