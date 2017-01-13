defmodule PagingTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.{Rows, Stream}

  setup_all %{keyspace: keyspace} do
    {:ok, conn} = Xandra.start_link()
    Xandra.execute!(conn, "USE #{keyspace}", [])

    statement = "CREATE TABLE alphabet (lang text, letter text, PRIMARY KEY (lang, letter))"
    Xandra.execute!(conn, statement, [])

    statement = """
    BEGIN BATCH
    INSERT INTO alphabet (lang, letter) VALUES ('en', 'Aa');
    INSERT INTO alphabet (lang, letter) VALUES ('en', 'Bb');
    INSERT INTO alphabet (lang, letter) VALUES ('en', 'Cc');
    INSERT INTO alphabet (lang, letter) VALUES ('en', 'Dd');
    INSERT INTO alphabet (lang, letter) VALUES ('en', 'Ee');
    INSERT INTO alphabet (lang, letter) VALUES ('en', 'Ff');
    INSERT INTO alphabet (lang, letter) VALUES ('en', 'Gg');
    INSERT INTO alphabet (lang, letter) VALUES ('en', 'Hh');
    INSERT INTO alphabet (lang, letter) VALUES ('en', 'Ii');
    INSERT INTO alphabet (lang, letter) VALUES ('en', 'Jj');
    APPLY BATCH
    """
    Xandra.execute!(conn, statement, [])

    :ok
  end

  test "manual paging", %{conn: conn} do
    query = Xandra.prepare!(conn, "SELECT letter FROM alphabet", [])

    assert {:ok, %Rows{} = rows} = Xandra.execute(conn, query, [], [page_size: 3])
    assert Enum.to_list(rows) == [
      %{"letter" => "Aa"}, %{"letter" => "Bb"}, %{"letter" => "Cc"}
    ]

    assert {:ok, %Rows{} = rows} = Xandra.execute(conn, query, [], [page_size: 2, cursor: rows])
    assert Enum.to_list(rows) == [
      %{"letter" => "Dd"}, %{"letter" => "Ee"}
    ]
  end

  test "streaming", %{conn: conn} do
    query = Xandra.prepare!(conn, "SELECT letter FROM alphabet", [])

    assert %Stream{} = stream = Xandra.stream!(conn, query, [], [page_size: 2])
    assert [rows1, rows2, rows3, rows4] = Enum.take(stream, 4)
    assert Enum.to_list(rows1) == [
      %{"letter" => "Aa"}, %{"letter" => "Bb"}
    ]
    assert Enum.to_list(rows2) == [
      %{"letter" => "Cc"}, %{"letter" => "Dd"}
    ]
    assert Enum.to_list(rows3) == [
      %{"letter" => "Ee"}, %{"letter" => "Ff"}
    ]
    assert Enum.to_list(rows4) == [
      %{"letter" => "Gg"}, %{"letter" => "Hh"}
    ]

    assert %Stream{} = stream = Xandra.stream!(conn, "SELECT letter FROM alphabet", [], [page_size: 2])
    assert [rows1, rows2] = Enum.take(stream, 2)
    assert Enum.to_list(rows1) == [
      %{"letter" => "Aa"}, %{"letter" => "Bb"}
    ]
    assert Enum.to_list(rows2) == [
      %{"letter" => "Cc"}, %{"letter" => "Dd"}
    ]
  end
end
