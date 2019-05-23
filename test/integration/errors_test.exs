defmodule ErrorsTest do
  use XandraTest.IntegrationCase

  alias Xandra.{Error, Frame, Protocol, Simple}

  test "each possible error", %{conn: conn} do
    assert {:error, reason} = Xandra.execute(conn, "")
    assert %Error{reason: :invalid_syntax} = reason

    assert {:error, reason} = Xandra.execute(conn, "USE unknown")
    assert %Error{reason: :invalid} = reason

    Xandra.execute!(conn, "CREATE TABLE errors (id int PRIMARY KEY, reason text)")
    assert {:error, reason} = Xandra.execute(conn, "CREATE TABLE errors (id int PRIMARY KEY)")
    assert %Error{reason: :already_exists} = reason

    assert {:error, reason} = Xandra.prepare(conn, "SELECT * FROM unknown")
    assert %Error{reason: :invalid} = reason
  end

  @tag protocol_version: :v4
  test "readout_failure error with fixture" do
    # response body "SELECT * FROM tombstones" from "test read_failure error" with a warning and a
    # read_failure error
    body =
      <<0, 1, 0, 151, 82, 101, 97, 100, 32, 48, 32, 108, 105, 118, 101, 32, 114, 111, 119, 115,
        32, 97, 110, 100, 32, 49, 48, 48, 48, 48, 49, 32, 116, 111, 109, 98, 115, 116, 111, 110,
        101, 32, 99, 101, 108, 108, 115, 32, 102, 111, 114, 32, 113, 117, 101, 114, 121, 32, 83,
        69, 76, 69, 67, 84, 32, 42, 32, 70, 82, 79, 77, 32, 120, 97, 110, 100, 114, 97, 95, 116,
        101, 115, 116, 95, 101, 114, 114, 111, 114, 115, 116, 101, 115, 116, 46, 116, 111, 109,
        98, 115, 116, 111, 110, 101, 115, 32, 87, 72, 69, 82, 69, 32, 32, 76, 73, 77, 73, 84, 32,
        49, 48, 48, 48, 48, 32, 40, 115, 101, 101, 32, 116, 111, 109, 98, 115, 116, 111, 110, 101,
        95, 119, 97, 114, 110, 95, 116, 104, 114, 101, 115, 104, 111, 108, 100, 41, 0, 0, 19, 0,
        0, 54, 79, 112, 101, 114, 97, 116, 105, 111, 110, 32, 102, 97, 105, 108, 101, 100, 32, 45,
        32, 114, 101, 99, 101, 105, 118, 101, 100, 32, 48, 32, 114, 101, 115, 112, 111, 110, 115,
        101, 115, 32, 97, 110, 100, 32, 49, 32, 102, 97, 105, 108, 117, 114, 101, 115, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0>>

    frame = %Frame{kind: :error, body: body, warning: true}

    query = %Simple{
      statement: "SELECT * FROM tombstones",
      values: nil,
      protocol_module: Protocol.V4
    }

    assert %Error{reason: :read_failure} = Protocol.V4.decode_response(frame, query, [])
  end

  @tag :slow
  test "read_failure error", %{conn: conn} do
    Xandra.execute!(conn, "CREATE TABLE tombstones (id int PRIMARY KEY, value text)")
    {:ok, insert} = Xandra.prepare(conn, "INSERT INTO tombstones (id, value) VALUES (?, ?)")
    {:ok, delete} = Xandra.prepare(conn, "DELETE FROM tombstones WHERE id = ?")

    # we need > 100000 tombstones to generate a read_failure error message
    for i <- 1..100_001 do
      Xandra.execute!(conn, insert, [i, "value"])
      Xandra.execute!(conn, delete, [i])
    end

    {:error, reason} = Xandra.execute(conn, "SELECT * FROM tombstones")
    assert %Error{reason: :read_failure} = reason
  end

  @tag protocol_version: :v4
  @tag :udf
  test "function_failure error", %{conn: conn} do
    statement = """
    CREATE FUNCTION thrower (x int) CALLED ON NULL INPUT
    RETURNS int LANGUAGE java AS 'throw new RuntimeException();'
    """

    assert {:ok, result} = Xandra.execute(conn, statement)
    assert {:error, reason} = Xandra.execute(conn, "SELECT thrower(code) FROM users")

    assert %Xandra.Error{
      message:
        "execution of 'xandra_test_preparedtest.thrower[int]' failed: java.lang.RuntimeException",
      reason: :function_failure
    }
  end

  test "errors are raised from bang! functions", %{conn: conn} do
    assert_raise Error, fn -> Xandra.prepare!(conn, "") end
    assert_raise Error, fn -> Xandra.execute!(conn, "USE unknown") end
  end
end
