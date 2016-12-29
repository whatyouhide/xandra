defmodule Xandra.TypeParserTest do
  use ExUnit.Case, async: true

  import Xandra.TypeParser, only: [parse: 1]

  test "parse/1 with valid types" do
    assert parse("uuid") == :uuid
    assert parse("text") == :text
    assert parse("list<int>") == {:list, :int}
    assert parse("map<int, text>") == {:map, :int, :text}
    assert parse("set<map<int, list<text>>>") == {:set, {:map, :int, {:list, :text}}}
    assert parse("map<list<int>, map<int, int>>") == {:map, {:list, :int}, {:map, :int, :int}}

    # Spaces here and there
    assert parse("map<int,text>") == {:map, :int, :text}
    assert parse("map<list<int>,map<int,int>>") == {:map, {:list, :int}, {:map, :int, :int}}
    assert parse(" map < int,   set < map < int, int  > > > ") == {:map, :int, {:set, {:map, :int, :int}}}

    # Mixed case
    assert parse("TEXT") == :text
    assert parse("MAP<INT, SET<TEXT>>") == {:map, :int, {:set, :text}}
    assert parse("List<tExT>") == {:list, :text}
  end

  test "parse/1 with invalid types" do
    assert_raise ArgumentError, ~s(invalid type " ": unexpected end of input), fn ->
      parse(" ")
    end

    assert_raise ArgumentError, ~s(invalid type "list<>": syntax error for empty subtype), fn ->
      parse("list<>")
    end

    assert_raise ArgumentError, ~s(invalid type "list<int>>": syntax error for misplaced >), fn ->
      parse("list<int>>")
    end

    assert_raise ArgumentError, ~s(invalid type "list<int,>": syntax error for empty subtype), fn ->
      parse("list<int,>")
    end

    assert_raise ArgumentError, ~s(invalid type "list< ,int>": syntax error for empty subtype), fn ->
      parse("list< ,int>")
    end

    assert_raise ArgumentError, ~s(invalid type "foo<bar>": unknown type "foo"), fn ->
      parse("foo<bar>")
    end
  end
end
