defmodule NotSetTest do
  use XandraTest.IntegrationCase, async: true, start_options: [protocol_version: :v4]

  test "not_set values in simple statements", %{conn: conn} do
    statement = "CREATE TABLE towns (id int PRIMARY KEY, name text, country text)"
    Xandra.execute!(conn, statement)

    statement = "INSERT INTO towns (id, name, country) VALUES (?, ?, ?)"

    assert %Xandra.Void{} =
             Xandra.execute!(conn, statement, [
               {"int", 1},
               {"text", "Somewhereville"},
               {"text", "Countryman"}
             ])

    statement = "SELECT id, country, WRITETIME(name), WRITETIME(country) FROM towns"
    assert [town] = Enum.to_list(Xandra.execute!(conn, statement))
    writetime = town["writetime(country)"]
    assert is_integer(writetime)
  end

  test "not_set values in prepared statements", %{conn: conn} do
    statement = "CREATE TABLE cars (id int PRIMARY KEY, name text)"
    Xandra.execute!(conn, statement)

    statement = "INSERT INTO cars (id, name) VALUES (?, ?)"
    prepared = Xandra.prepare!(conn, statement)

    Xandra.execute!(conn, prepared, [1, "Tesla"])
    Xandra.execute!(conn, prepared, [2, "BMW"])
    Xandra.execute!(conn, prepared, [3, "T3"])

    page = Xandra.execute!(conn, "SELECT id, name FROM cars")

    assert Enum.sort(page, &(&1["id"] <= &2["id"])) == [
             %{"id" => 1, "name" => "Tesla"},
             %{"id" => 2, "name" => "BMW"},
             %{"id" => 3, "name" => "T3"}
           ]

    Xandra.execute!(conn, prepared, [1, "Mercedes"])
    Xandra.execute!(conn, prepared, [2, nil])
    Xandra.execute!(conn, prepared, [3, :not_set])

    page = Xandra.execute!(conn, "SELECT id, name FROM cars")

    assert Enum.sort(page, &(&1["id"] <= &2["id"])) == [
             %{"id" => 1, "name" => "Mercedes"},
             %{"id" => 2, "name" => nil},
             %{"id" => 3, "name" => "T3"}
           ]
  end
end
