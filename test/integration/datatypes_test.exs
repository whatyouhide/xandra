defmodule DataTypesTest do
  use XandraTest.IntegrationCase, async: true

  test "primitive datatypes", %{conn: conn} do
    statement = """
    CREATE TABLE primitives
    (id int PRIMARY KEY,
     ascii ascii,
     bigint bigint,
     blob blob,
     boolean boolean,
     decimal decimal,
     double double,
     float float,
     inet inet,
     int int,
     text text,
     timestamp timestamp,
     timeuuid timeuuid,
     uuid uuid,
     varchar varchar,
     varint varint)
    """
    Xandra.execute!(conn, statement, [])

    statement = """
    INSERT INTO primitives
    (id,
     ascii,
     bigint,
     blob,
     boolean,
     decimal,
     double,
     float,
     inet,
     int,
     text,
     timestamp,
     timeuuid,
     uuid,
     varchar,
     varint)
    VALUES
    (#{"?" |> List.duplicate(16) |> Enum.join(", ")})
    """

    values = [
      {"int", 1},
      {"ascii", nil},
      {"bigint", nil},
      {"blob", nil},
      {"boolean", nil},
      {"decimal", nil},
      {"double", nil},
      {"float", nil},
      {"inet", nil},
      {"int", nil},
      {"text", nil},
      {"timestamp", nil},
      {"timeuuid", nil},
      {"uuid", nil},
      {"varchar", nil},
      {"varint", nil},
    ]
    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM primitives WHERE id = 1", [])
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 1
    assert Map.fetch!(row, "ascii") == nil
    assert Map.fetch!(row, "bigint") == nil
    assert Map.fetch!(row, "blob") == nil
    assert Map.fetch!(row, "boolean") == nil
    assert Map.fetch!(row, "decimal") == nil
    assert Map.fetch!(row, "double") == nil
    assert Map.fetch!(row, "float") == nil
    assert Map.fetch!(row, "inet") == nil
    assert Map.fetch!(row, "int") == nil
    assert Map.fetch!(row, "text") == nil
    assert Map.fetch!(row, "timestamp") == nil
    assert Map.fetch!(row, "timeuuid") == nil
    assert Map.fetch!(row, "uuid") == nil
    assert Map.fetch!(row, "varchar") == nil
    assert Map.fetch!(row, "varint") == nil

    values = [
      {"int", 2},
      {"ascii", "ascii"},
      {"bigint", -1000000000},
      {"blob", <<0x00FF::16>>},
      {"boolean", true},
      {"decimal", {1323, -2}},
      {"double", 3.1415},
      {"float", -1.25},
      {"inet", {192, 168, 0, 1}},
      {"int", -42},
      {"text", "эликсир"},
      {"timestamp", -2167219200},
      {"timeuuid", "fe2b4360-28c6-11e2-81c1-0800200c9a66"},
      {"uuid", "00b69180-d0e1-11e2-8b8b-0800200c9a66"},
      {"varchar", "тоже эликсир"},
      {"varint", -6789065678192312391879827349},
    ]
    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM primitives WHERE id = 2", [])
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "ascii") == "ascii"
    assert Map.fetch!(row, "bigint") == -1000000000
    assert Map.fetch!(row, "blob") == <<0, 0xFF>>
    assert Map.fetch!(row, "boolean") == true
    assert Map.fetch!(row, "decimal") == {1323, -2}
    assert Map.fetch!(row, "double") == 3.1415
    assert Map.fetch!(row, "float") == -1.25
    assert Map.fetch!(row, "inet") == {192, 168, 0, 1}
    assert Map.fetch!(row, "int") == -42
    assert Map.fetch!(row, "text") == "эликсир"
    assert Map.fetch!(row, "timestamp") == -2167219200
    assert Map.fetch!(row, "timeuuid") == <<254, 43, 67, 96, 40, 198, 17, 226, 129, 193, 8, 0, 32, 12, 154, 102>>
    assert Map.fetch!(row, "uuid") == <<0, 182, 145, 128, 208, 225, 17, 226, 139, 139, 8, 0, 32, 12, 154, 102>>
    assert Map.fetch!(row, "varchar") == "тоже эликсир"
    assert Map.fetch!(row, "varint") == -6789065678192312391879827349
  end

  test "collection datatypes", %{conn: conn} do
    statement = """
    CREATE TABLE collections
    (id int PRIMARY KEY,
     list_of_int list<int>,
     map_of_int_to_text map<int, text>,
     set_of_int set<int>,
     tuple_of_int_and_text tuple<int, text>)
    """
    Xandra.execute!(conn, statement, [])

    statement = """
    INSERT INTO collections
    (id,
     list_of_int,
     map_of_int_to_text,
     set_of_int,
     tuple_of_int_and_text)
    VALUES
    (#{"?" |> List.duplicate(5) |> Enum.join(", ")})
    """

    values = [
      {"int", 1},
      {"list<int>", nil},
      {"map<int, text>", nil},
      {"set<int>", nil},
      {"tuple<int, text>", nil},
    ]
    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM collections WHERE id = 1", [])
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 1
    assert Map.fetch!(row, "list_of_int") == nil
    assert Map.fetch!(row, "map_of_int_to_text") == nil
    assert Map.fetch!(row, "set_of_int") == nil
    assert Map.fetch!(row, "tuple_of_int_and_text") == nil

    values = [
      {"int", 2},
      {"list<int>", [24, 42]},
      {"map<int, text>", %{24 => "24", 42 => "42"}},
      {"set<int>", MapSet.new([42, 24])},
      {"tuple<int, text>", {24, "42"}},
    ]
    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM collections WHERE id = 2", [])
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 2
    assert Map.fetch!(row, "list_of_int") == [24, 42]
    assert Map.fetch!(row, "map_of_int_to_text") == %{24 => "24", 42 => "42"}
    assert Map.fetch!(row, "set_of_int") == MapSet.new([42, 24])
    assert Map.fetch!(row, "tuple_of_int_and_text") == {24, "42"}

    # Empty collections
    values = [
      {"int", 3},
      {"list<int>", []},
      {"map<int, text>", %{}},
      {"set<int>", MapSet.new([])},
      # Tuples do not have empty representation
      {"tuple<int, text>", nil},
    ]
    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM collections WHERE id = 3", [])
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 3
    assert Map.fetch!(row, "list_of_int") == nil
    assert Map.fetch!(row, "map_of_int_to_text") == nil
    assert Map.fetch!(row, "set_of_int") == nil
  end

  test "user-defined types", %{conn: conn} do
    statement = """
    CREATE TYPE IF NOT EXISTS full_name
    (first_name text,
     last_name text)
    """
    Xandra.execute!(conn, statement)

    statement = """
    CREATE TYPE IF NOT EXISTS profile
    (nickname text,
     real_name frozen<full_name>)
    """
    Xandra.execute!(conn, statement)

    statement = """
    CREATE TABLE users
    (id int PRIMARY KEY,
     profile frozen<profile>)
    """
    Xandra.execute!(conn, statement)

    statement = "INSERT INTO users (id, profile) VALUES (?, ?)"
    foo_profile = %{
      "nickname" => "foo",
      "real_name" => %{"first_name" => "Kung", "last_name" => "Foo"},
    }
    bar_profile = %{
      "nickname" => "bar",
      "real_name" => %{"last_name" => "Bar"},
    }
    prepared = Xandra.prepare!(conn, statement)
    Xandra.execute!(conn, prepared, [1, foo_profile])
    Xandra.execute!(conn, prepared, [2, bar_profile])

    statement = "SELECT id, profile FROM users"
    page = Xandra.execute!(conn, statement)
    assert [foo, bar] = Enum.to_list(page)
    assert Map.fetch!(foo, "id") == 1
    assert Map.fetch!(foo, "profile") == foo_profile
    assert Map.fetch!(bar, "id") == 2
    assert Map.fetch!(bar, "profile") == %{"nickname" => "bar", "real_name" => %{"first_name" => nil, "last_name" => "Bar"}}
  end
end
