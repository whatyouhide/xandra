defmodule Xandra.TypeParserTest do
  use ExUnit.Case, async: true

  import Xandra.TypeParser, only: [parse: 1]

  test "parse/1" do
    assert parse("uuid") == :uuid
    assert parse("text") == :text
    assert parse("list<int>") == {:list, :int}
    assert parse("map<int, text>") == {:map, :int, :text}
    assert parse("set<map<int, list<text>>>") == {:set, {:map, :int, {:list, :text}}}
    assert parse("map<list<int>, map<int, int>>") == {:map, {:list, :int}, {:map, :int, :int}}

    # Mixed case
    assert parse("TEXT") == :text
    assert parse("MAP<INT, SET<TEXT>>")
  end
end
