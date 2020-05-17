defmodule DataTypesTest do
  use XandraTest.IntegrationCase, async: true

  test "primitive datatypes", %{conn: conn} do
    statement = """
    CREATE TABLE primitives
    (id int PRIMARY KEY,
     an_ascii ascii,
     a_bigint bigint,
     a_blob blob,
     a_boolean boolean,
     a_decimal decimal,
     a_double double,
     a_float float,
     an_inet inet,
     an_int int,
     a_smallint smallint,
     a_text text,
     a_time time,
     a_timestamp timestamp,
     a_timeuuid timeuuid,
     a_tinyint tinyint,
     a_uuid uuid,
     a_varchar varchar,
     a_varint varint)
    """

    Xandra.execute!(conn, statement)

    statement = """
    INSERT INTO primitives
    (id,
     an_ascii,
     a_bigint,
     a_blob,
     a_boolean,
     a_decimal,
     a_double,
     a_float,
     an_inet,
     an_int,
     a_smallint,
     a_text,
     a_timeuuid,
     a_tinyint,
     a_uuid,
     a_varchar,
     a_varint)
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
    assert Map.fetch!(row, "an_ascii") == nil
    assert Map.fetch!(row, "a_bigint") == nil
    assert Map.fetch!(row, "a_blob") == nil
    assert Map.fetch!(row, "a_boolean") == nil
    assert Map.fetch!(row, "a_decimal") == nil
    assert Map.fetch!(row, "a_double") == nil
    assert Map.fetch!(row, "a_float") == nil
    assert Map.fetch!(row, "an_inet") == nil
    assert Map.fetch!(row, "an_int") == nil
    assert Map.fetch!(row, "a_smallint") == nil
    assert Map.fetch!(row, "a_text") == nil
    assert Map.fetch!(row, "a_timeuuid") == nil
    assert Map.fetch!(row, "a_tinyint") == nil
    assert Map.fetch!(row, "a_uuid") == nil
    assert Map.fetch!(row, "a_varchar") == nil
    assert Map.fetch!(row, "a_varint") == nil

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
    assert Map.fetch!(row, "an_ascii") == "ascii"
    assert Map.fetch!(row, "a_bigint") == -1_000_000_000
    assert Map.fetch!(row, "a_blob") == <<0, 0xFF>>
    assert Map.fetch!(row, "a_boolean") == true
    assert Map.fetch!(row, "a_decimal") == {1323, -2}
    assert Map.fetch!(row, "a_double") == 3.1415
    assert Map.fetch!(row, "a_float") == -1.25
    assert Map.fetch!(row, "an_inet") == {192, 168, 0, 1}
    assert Map.fetch!(row, "an_int") == -42
    assert Map.fetch!(row, "a_smallint") == -33
    assert Map.fetch!(row, "a_text") == "эликсир"
    assert Map.fetch!(row, "a_timeuuid") == "fe2b4360-28c6-11e2-81c1-0800200c9a66"
    assert Map.fetch!(row, "a_tinyint") == -21
    assert Map.fetch!(row, "a_uuid") == "00b69180-d0e1-11e2-8b8b-0800200c9a66"
    assert Map.fetch!(row, "a_varchar") == "тоже эликсир"
    assert Map.fetch!(row, "a_varint") == -6_789_065_678_192_312_391_879_827_349
  end

  test "zero-byte value for string types", %{conn: conn} do
    statement = """
    CREATE TABLE string_with_zero_bytes
    (id int PRIMARY KEY,
     an_ascii ascii,
     a_blob blob,
     a_text text,
     a_varchar varchar)
    """

    Xandra.execute!(conn, statement)

    statement = """
    INSERT INTO string_with_zero_bytes
    (id,
     an_ascii,
     a_blob,
     a_text,
     a_varchar)
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
    assert Map.fetch!(row, "an_ascii") == ""
    assert Map.fetch!(row, "a_blob") == ""
    assert Map.fetch!(row, "a_text") == ""
    assert Map.fetch!(row, "a_varchar") == ""
  end

  test "calendar types", %{conn: conn} do
    statement = """
    CREATE TABLE festivities
    (id int PRIMARY KEY,
     a_date date,
     a_time time,
     a_timestamp timestamp)
    """

    Xandra.execute!(conn, statement)

    statement = """
    INSERT INTO festivities
    (id,
     a_date,
     a_time,
     a_timestamp)
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
    assert Map.fetch!(row, "a_date") == nil
    assert Map.fetch!(row, "a_time") == nil
    assert Map.fetch!(row, "a_timestamp") == nil

    datetime = DateTime.from_naive!(~N[2016-05-24 13:26:08.003], "Etc/UTC")

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
    assert Map.fetch!(row, "a_date") == ~D[2017-09-11]
    assert Map.fetch!(row, "a_time") == ~T[20:13:50.000004]
    assert Map.fetch!(row, "a_timestamp") == datetime

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
    assert Map.fetch!(row, "a_date") == 1_358_013_521
    assert Map.fetch!(row, "a_time") == 1_358_013_521
    assert Map.fetch!(row, "a_timestamp") == -2_167_219_200
  end

  test "decimal type with formats", %{conn: conn} do
    statement = """
    CREATE TABLE decs (id int PRIMARY KEY, a_decimal decimal)
    """

    Xandra.execute!(conn, statement)

    statement = """
    INSERT INTO decs (id, a_decimal) VALUES (?, ?)
    """

    # 95.343
    decimal_as_tuple = {95343, 3}

    values = [
      {"int", 1},
      {"decimal", decimal_as_tuple}
    ]

    Xandra.execute!(conn, statement, values)

    page = Xandra.execute!(conn, "SELECT * FROM decs WHERE id = 1")
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 1
    assert Map.fetch!(row, "a_decimal") == decimal_as_tuple

    # 95.343
    decimal_as_decimal = Decimal.new(1, 95343, -3)

    values = [
      {"int", 2},
      {"decimal", decimal_as_decimal}
    ]

    Xandra.execute!(conn, statement, values)

    page = Xandra.execute!(conn, "SELECT * FROM decs WHERE id = 2", [], decimal_format: :decimal)
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 2
    assert Map.fetch!(row, "a_decimal") == Decimal.new(1, 95343, -3)

    page = Xandra.execute!(conn, "SELECT * FROM decs WHERE id = 2", [], decimal_format: :tuple)
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 2
    assert Map.fetch!(row, "a_decimal") == decimal_as_tuple

    # -5.0
    negative_decimal = Decimal.new("-5.0")

    values = [
      {"int", 3},
      {"decimal", negative_decimal}
    ]

    Xandra.execute!(conn, statement, values)

    page = Xandra.execute!(conn, "SELECT * FROM decs WHERE id = 3", [], decimal_format: :decimal)
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 3
    assert Map.fetch!(row, "a_decimal") == Decimal.new("-5.0")
    assert row |> Map.fetch!("a_decimal") |> Decimal.negative?()
  end

  test "uuid/timeuuid types with format", %{conn: conn} do
    statement = """
    CREATE TABLE uuids (id int PRIMARY KEY, a_uuid uuid, a_timeuuid timeuuid)
    """

    Xandra.execute!(conn, statement)

    statement = """
    INSERT INTO uuids (id, a_uuid, a_timeuuid) VALUES (?, ?, ?)
    """

    uuid_as_binary = <<0, 182, 145, 128, 208, 225, 17, 226, 139, 139, 8, 0, 32, 12, 154, 102>>
    timeuuid_as_binary = <<0, 182, 145, 128, 208, 225, 17, 226, 139, 139, 8, 0, 32, 12, 154, 102>>

    values = [
      {"int", 1},
      {"uuid", uuid_as_binary},
      {"timeuuid", timeuuid_as_binary}
    ]

    Xandra.execute!(conn, statement, values)

    options = [uuid_format: :binary, timeuuid_format: :binary]
    page = Xandra.execute!(conn, "SELECT * FROM uuids WHERE id = 1", [], options)
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 1
    assert Map.fetch!(row, "a_uuid") == uuid_as_binary
    assert Map.fetch!(row, "a_timeuuid") == timeuuid_as_binary

    uuid_as_string = "fe2b4360-28c6-11e2-81c1-0800200c9a66"
    timeuuid_as_string = "fe2b4360-28c6-11e2-81c1-0800200c9a67"

    values = [
      {"int", 2},
      {"uuid", uuid_as_string},
      {"timeuuid", timeuuid_as_string}
    ]

    Xandra.execute!(conn, statement, values)

    options = [uuid_format: :string, timeuuid_format: :string]
    page = Xandra.execute!(conn, "SELECT * FROM uuids WHERE id = 2", [], options)
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 2
    assert Map.fetch!(row, "a_uuid") == uuid_as_string
    assert Map.fetch!(row, "a_timeuuid") == timeuuid_as_string
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

  test "user-defined types (reduced)", %{conn: conn} do
    statement = """
    CREATE TYPE full_name_r
    (first_name text,
     last_name text)
    """

    Xandra.execute!(conn, statement)

    statement = """
    CREATE TYPE profile_r
    (nickname text,
     full_name frozen<full_name_r>)
    """

    Xandra.execute!(conn, statement)

    statement = """
    CREATE TABLE users_r
    (id int PRIMARY KEY,
     profile frozen<profile_r>)
    """

    Xandra.execute!(conn, statement)

    statement = "INSERT INTO users_r (id, profile) VALUES (?, ?)"

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

    statement = "SELECT id, profile FROM users_r"
    page = Xandra.execute!(conn, statement)
    assert [foo, bar] = Enum.sort(page, &(&1["id"] <= &2["id"]))
    assert Map.fetch!(foo, "id") == 1
    assert Map.fetch!(foo, "profile") == foo_profile
    assert Map.fetch!(bar, "id") == 2

    assert Map.fetch!(bar, "profile") == %{
             "nickname" => "bar",
             "full_name" => %{"first_name" => nil, "last_name" => "Bar"}
           }
  end

  @tag :cosmosdb_unsupported
  test "user-defined types (full)", %{conn: conn} do
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

  test "inet type", %{conn: conn} do
    statement = """
    CREATE TABLE inets
    (id int PRIMARY KEY,
     addr inet,
     addrv6 inet)
    """

    addr = {127, 0, 0, 1}
    addrv6 = {64935, 43320, 23550, 24486, 0, 0, 0, 49}

    Xandra.execute!(conn, statement)

    statement = """
    INSERT INTO inets (id, addr, addrv6) VALUES (?, ?, ?)
    """

    values = [
      {"int", 1},
      {"inet", addr},
      {"inet", addrv6}
    ]

    Xandra.execute!(conn, statement, values)

    page = Xandra.execute!(conn, "SELECT * FROM inets WHERE id = 1")
    assert [row] = Enum.to_list(page)
    assert Map.fetch!(row, "id") == 1
    assert Map.fetch!(row, "addr") == addr
    assert Map.fetch!(row, "addrv6") == addrv6
  end
end
