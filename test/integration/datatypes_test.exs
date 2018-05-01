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
     smallint smallint,
     text text,
     time time,
     timestamp timestamp,
     timeuuid timeuuid,
     tinyint tinyint,
     uuid uuid,
     varchar varchar,
     varint varint)
    """

    Xandra.execute!(conn, statement)

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
     smallint,
     text,
     timeuuid,
     tinyint,
     uuid,
     varchar,
     varint)
    VALUES
    (#{"?" |> List.duplicate(17) |> Enum.join(", ")})
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
      {"smallint", nil},
      {"text", nil},
      {"timeuuid", nil},
      {"tinyint", nil},
      {"uuid", nil},
      {"varchar", nil},
      {"varint", nil}
    ]

    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM primitives WHERE id = 1")
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
    assert Map.fetch!(row, "smallint") == nil
    assert Map.fetch!(row, "text") == nil
    assert Map.fetch!(row, "timeuuid") == nil
    assert Map.fetch!(row, "tinyint") == nil
    assert Map.fetch!(row, "uuid") == nil
    assert Map.fetch!(row, "varchar") == nil
    assert Map.fetch!(row, "varint") == nil

    values = [
      {"int", 2},
      {"ascii", "ascii"},
      {"bigint", -1_000_000_000},
      {"blob", <<0x00FF::16>>},
      {"boolean", true},
      {"decimal", {1323, -2}},
      {"double", 3.1415},
      {"float", -1.25},
      {"inet", {192, 168, 0, 1}},
      {"int", -42},
      {"smallint", -33},
      {"text", "эликсир"},
      {"timeuuid", "fe2b4360-28c6-11e2-81c1-0800200c9a66"},
      {"tinyint", -21},
      {"uuid", <<0, 182, 145, 128, 208, 225, 17, 226, 139, 139, 8, 0, 32, 12, 154, 102>>},
      {"varchar", "тоже эликсир"},
      {"varint", -6_789_065_678_192_312_391_879_827_349}
    ]

    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM primitives WHERE id = 2")
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "ascii") == "ascii"
    assert Map.fetch!(row, "bigint") == -1_000_000_000
    assert Map.fetch!(row, "blob") == <<0, 0xFF>>
    assert Map.fetch!(row, "boolean") == true
    assert Map.fetch!(row, "decimal") == {1323, -2}
    assert Map.fetch!(row, "double") == 3.1415
    assert Map.fetch!(row, "float") == -1.25
    assert Map.fetch!(row, "inet") == {192, 168, 0, 1}
    assert Map.fetch!(row, "int") == -42
    assert Map.fetch!(row, "smallint") == -33
    assert Map.fetch!(row, "text") == "эликсир"

    assert Map.fetch!(row, "timeuuid") ==
             <<254, 43, 67, 96, 40, 198, 17, 226, 129, 193, 8, 0, 32, 12, 154, 102>>

    assert Map.fetch!(row, "tinyint") == -21

    assert Map.fetch!(row, "uuid") ==
             <<0, 182, 145, 128, 208, 225, 17, 226, 139, 139, 8, 0, 32, 12, 154, 102>>

    assert Map.fetch!(row, "varchar") == "тоже эликсир"
    assert Map.fetch!(row, "varint") == -6_789_065_678_192_312_391_879_827_349
  end

  test "zero-byte value for string types", %{conn: conn} do
    statement = """
    CREATE TABLE string_with_zero_bytes
    (id int PRIMARY KEY,
     ascii ascii,
     blob blob,
     text text,
     varchar varchar)
    """

    Xandra.execute!(conn, statement)

    statement = """
    INSERT INTO string_with_zero_bytes
    (id,
     ascii,
     blob,
     text,
     varchar)
    VALUES
    (#{"?" |> List.duplicate(5) |> Enum.join(", ")})
    """

    values = [
      {"int", 1},
      {"ascii", ""},
      {"blob", ""},
      {"text", ""},
      {"varchar", ""}
    ]

    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM string_with_zero_bytes WHERE id = 1")
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 1
    assert Map.fetch!(row, "ascii") == ""
    assert Map.fetch!(row, "blob") == ""
    assert Map.fetch!(row, "text") == ""
    assert Map.fetch!(row, "varchar") == ""
  end

  test "calendar types", %{conn: conn} do
    statement = """
    CREATE TABLE festivities
    (id int PRIMARY KEY,
     date date,
     time time,
     timestamp timestamp)
    """

    Xandra.execute!(conn, statement)

    statement = """
    INSERT INTO festivities
    (id,
     date,
     time,
     timestamp)
    VALUES
    (#{"?" |> List.duplicate(4) |> Enum.join(", ")})
    """

    values = [
      {"int", 1},
      {"date", nil},
      {"time", nil},
      {"timestamp", nil}
    ]

    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM festivities WHERE id = 1")
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 1
    assert Map.fetch!(row, "date") == nil
    assert Map.fetch!(row, "time") == nil
    assert Map.fetch!(row, "timestamp") == nil

    # NOTE: DateTime.from_naive!/2 was introduced in Elixir v1.4.
    datetime =
      if Code.ensure_loaded?(DateTime) and function_exported?(DateTime, :from_naive!, 2) do
        DateTime.from_naive!(~N[2016-05-24 13:26:08.003], "Etc/UTC")
      else
        fields =
          ~N[2016-05-24 13:26:08.003]
          |> Map.from_struct()
          |> Map.put(:std_offset, 0)
          |> Map.put(:utc_offset, 0)
          |> Map.put(:zone_abbr, "UTC")
          |> Map.put(:time_zone, "Etc/UTC")

        struct(DateTime, fields)
      end

    values = [
      {"int", 2},
      {"date", ~D[2017-09-11]},
      {"time", ~T[20:13:50.000004]},
      {"timestamp", datetime}
    ]

    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM festivities WHERE id = 2")
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 2
    assert Map.fetch!(row, "date") == ~D[2017-09-11]
    assert Map.fetch!(row, "time") == ~T[20:13:50.000004]
    assert Map.fetch!(row, "timestamp") == datetime

    values = [
      {"int", 3},
      {"date", 1_358_013_521},
      {"time", 1_358_013_521},
      {"timestamp", -2_167_219_200}
    ]

    Xandra.execute!(conn, statement, values)

    options = [
      date_format: :integer,
      time_format: :integer,
      timestamp_format: :integer
    ]

    page = Xandra.execute!(conn, "SELECT * FROM festivities WHERE id = 3", [], options)
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 3
    assert Map.fetch!(row, "date") == 1_358_013_521
    assert Map.fetch!(row, "time") == 1_358_013_521
    assert Map.fetch!(row, "timestamp") == -2_167_219_200
  end

  test "collection datatypes", %{conn: conn} do
    statement = """
    CREATE TABLE collections
    (id int PRIMARY KEY,
     list_of_int list<int>,
     map_of_int_to_text map<int, text>,
     set_of_tinyint set<tinyint>,
     tuple_of_int_and_text tuple<int, text>)
    """

    Xandra.execute!(conn, statement)

    statement = """
    INSERT INTO collections
    (id,
     list_of_int,
     map_of_int_to_text,
     set_of_tinyint,
     tuple_of_int_and_text)
    VALUES
    (#{"?" |> List.duplicate(5) |> Enum.join(", ")})
    """

    values = [
      {"int", 1},
      {"list<int>", nil},
      {"map<int, text>", nil},
      {"set<tinyint>", nil},
      {"tuple<int, text>", nil}
    ]

    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM collections WHERE id = 1")
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 1
    assert Map.fetch!(row, "list_of_int") == nil
    assert Map.fetch!(row, "map_of_int_to_text") == nil
    assert Map.fetch!(row, "set_of_tinyint") == nil
    assert Map.fetch!(row, "tuple_of_int_and_text") == nil

    values = [
      {"int", 2},
      {"list<int>", [24, 42]},
      {"map<int, text>", %{24 => "24", 42 => "42"}},
      {"set<tinyint>", MapSet.new([42, 24])},
      {"tuple<int, text>", {24, "42"}}
    ]

    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM collections WHERE id = 2")
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 2
    assert Map.fetch!(row, "list_of_int") == [24, 42]
    assert Map.fetch!(row, "map_of_int_to_text") == %{24 => "24", 42 => "42"}
    assert Map.fetch!(row, "set_of_tinyint") == MapSet.new([42, 24])
    assert Map.fetch!(row, "tuple_of_int_and_text") == {24, "42"}

    # Empty collections
    values = [
      {"int", 3},
      {"list<int>", []},
      {"map<tinyint, text>", %{}},
      {"set<tinyint>", MapSet.new([])},
      # Tuples do not have empty representation
      {"tuple<int, text>", nil}
    ]

    Xandra.execute!(conn, statement, values)
    page = Xandra.execute!(conn, "SELECT * FROM collections WHERE id = 3")
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 3
    assert Map.fetch!(row, "list_of_int") == nil
    assert Map.fetch!(row, "map_of_int_to_text") == nil
    assert Map.fetch!(row, "set_of_tinyint") == nil
  end

  test "user-defined types", %{conn: conn} do
    statement = """
    CREATE TYPE full_name
    (first_name text,
     last_name text)
    """

    Xandra.execute!(conn, statement)

    statement = """
    CREATE TYPE profile
    (nickname text,
     full_name frozen<full_name>)
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
      "full_name" => %{"first_name" => "Kung", "last_name" => "Foo"}
    }

    bar_profile = %{
      "nickname" => "bar",
      "full_name" => %{"last_name" => "Bar"}
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

    assert Map.fetch!(bar, "profile") == %{
             "nickname" => "bar",
             "full_name" => %{"first_name" => nil, "last_name" => "Bar"}
           }

    statement = """
    ALTER TYPE profile ADD email text
    """

    Xandra.execute!(conn, statement)

    statement = """
    ALTER TYPE profile ADD age int
    """

    Xandra.execute!(conn, statement)

    statement = "INSERT INTO users (id, profile) VALUES (?, ?)"

    baz_profile = %{
      "nickname" => "baz",
      "full_name" => %{"first_name" => "See", "last_name" => "Baz"},
      "email" => "baz@example.com"
    }

    prepared = Xandra.prepare!(conn, statement)
    Xandra.execute!(conn, prepared, [3, baz_profile])

    statement = "SELECT id, profile FROM users"
    page = Xandra.execute!(conn, statement)

    assert [foo, bar, baz] = Enum.to_list(page)
    assert Map.fetch!(foo, "id") == 1

    assert Map.fetch!(foo, "profile") ==
             foo_profile
             |> Map.put("email", nil)
             |> Map.put("age", nil)

    assert Map.fetch!(bar, "id") == 2

    assert Map.fetch!(bar, "profile") ==
             bar_profile
             |> Map.put("email", nil)
             |> Map.put("age", nil)
             |> Map.update!("full_name", &Map.put(&1, "first_name", nil))

    assert Map.fetch!(baz, "id") == 3
    assert Map.fetch!(baz, "profile") == Map.put(baz_profile, "age", nil)
  end

  test "counter type", %{conn: conn} do
    statement = """
    CREATE TABLE views
    (id int PRIMARY KEY,
     total counter)
    """

    Xandra.execute!(conn, statement)

    statement = "UPDATE views SET total = total + 4 WHERE id = 1"
    Xandra.execute!(conn, statement)

    page = Xandra.execute!(conn, "SELECT * FROM views WHERE id = 1")
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 1
    assert Map.fetch!(row, "total") == 4
  end
end
