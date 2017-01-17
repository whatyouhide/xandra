defmodule PagingTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.{Page, PagesStream}

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

    assert {:ok, %Page{} = page} = Xandra.execute(conn, query, [], [page_size: 3])
    assert Enum.to_list(page) == [
      %{"letter" => "Aa"}, %{"letter" => "Bb"}, %{"letter" => "Cc"}
    ]
    assert Page.more_pages_available?(page) == true

    assert {:ok, %Page{} = page} = Xandra.execute(conn, query, [], [page_size: 2, cursor: page])
    assert Enum.to_list(page) == [
      %{"letter" => "Dd"}, %{"letter" => "Ee"}
    ]
    assert Page.more_pages_available?(page) == true

    assert {:ok, %Page{} = page} = Xandra.execute(conn, query, [], [page_size: 6, cursor: page])
    assert Enum.count(page) == 5
    assert Page.more_pages_available?(page) == false
  end

  test "streamingpages", %{conn: conn} do
    query = Xandra.prepare!(conn, "SELECT letter FROM alphabet", [])

    assert %PagesStream{} = stream = Xandra.stream_pages!(conn, query, [], [page_size: 2])
    assert [page1, page2, page3, page4] = Enum.take(stream, 4)
    assert Enum.to_list(page1) == [
      %{"letter" => "Aa"}, %{"letter" => "Bb"}
    ]
    assert Enum.to_list(page2) == [
      %{"letter" => "Cc"}, %{"letter" => "Dd"}
    ]
    assert Enum.to_list(page3) == [
      %{"letter" => "Ee"}, %{"letter" => "Ff"}
    ]
    assert Enum.to_list(page4) == [
      %{"letter" => "Gg"}, %{"letter" => "Hh"}
    ]

    assert %PagesStream{} = stream = Xandra.stream_pages!(conn, "SELECT letter FROM alphabet", [], [page_size: 2])
    assert [page1, page2] = Enum.take(stream, 2)
    assert Enum.to_list(page1) == [
      %{"letter" => "Aa"}, %{"letter" => "Bb"}
    ]
    assert Enum.to_list(page2) == [
      %{"letter" => "Cc"}, %{"letter" => "Dd"}
    ]
  end

  test "inspecting Xandra.PagesStream structs", %{conn: conn} do
    pages_stream = Xandra.stream_pages!(conn, "SELECT * FROM alphabet", _params = [])
    assert inspect(pages_stream) == ~s(#Xandra.PagesStream<[query: "SELECT * FROM alphabet", params: [], options: []]>)
  end
end
