defmodule PagingTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.{Page, PageStream}

  setup_all %{keyspace: keyspace, setup_conn: setup_conn} do
    Xandra.execute!(setup_conn, "USE #{keyspace}")

    statement = "CREATE TABLE alphabet (lang text, letter text, PRIMARY KEY (lang, letter))"
    Xandra.execute!(setup_conn, statement)

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

    Xandra.execute!(setup_conn, statement)

    :ok
  end

  test "manual paging", %{conn: conn, keyspace: keyspace} do
    query = Xandra.prepare!(conn, "SELECT letter FROM #{keyspace}.alphabet")

    options = [page_size: 3]

    assert {:ok, %Page{paging_state: paging_state} = page} =
             Xandra.execute(conn, query, [], options)

    assert Enum.to_list(page) == [
             %{"letter" => "Aa"},
             %{"letter" => "Bb"},
             %{"letter" => "Cc"}
           ]

    assert paging_state != nil

    options = [page_size: 2, paging_state: paging_state]

    assert {:ok, %Page{paging_state: paging_state} = page} =
             Xandra.execute(conn, query, [], options)

    assert Enum.to_list(page) == [
             %{"letter" => "Dd"},
             %{"letter" => "Ee"}
           ]

    assert paging_state != nil

    options = [page_size: 6, paging_state: paging_state]

    assert {:ok, %Page{paging_state: paging_state} = page} =
             Xandra.execute(conn, query, [], options)

    assert Enum.count(page) == 5
    assert paging_state == nil

    assert_raise ArgumentError, "no more pages are available", fn ->
      Xandra.execute(conn, query, [], paging_state: nil)
    end
  end

  test "streaming pages", %{conn: conn, keyspace: keyspace} do
    query = Xandra.prepare!(conn, "SELECT letter FROM #{keyspace}.alphabet")

    assert %PageStream{} = stream = Xandra.stream_pages!(conn, query, [], page_size: 2)
    assert [page1, page2, page3, page4] = Enum.take(stream, 4)

    assert Enum.to_list(page1) == [
             %{"letter" => "Aa"},
             %{"letter" => "Bb"}
           ]

    assert Enum.to_list(page2) == [
             %{"letter" => "Cc"},
             %{"letter" => "Dd"}
           ]

    assert Enum.to_list(page3) == [
             %{"letter" => "Ee"},
             %{"letter" => "Ff"}
           ]

    assert Enum.to_list(page4) == [
             %{"letter" => "Gg"},
             %{"letter" => "Hh"}
           ]

    assert %PageStream{} =
             stream = Xandra.stream_pages!(conn, "SELECT letter FROM alphabet", [], page_size: 2)

    assert [page1, page2] = Enum.take(stream, 2)

    assert Enum.to_list(page1) == [
             %{"letter" => "Aa"},
             %{"letter" => "Bb"}
           ]

    assert Enum.to_list(page2) == [
             %{"letter" => "Cc"},
             %{"letter" => "Dd"}
           ]

    # Also other functions in the Enumerable module:
    refute Enum.member?(stream, make_ref())
    assert Enum.count(stream) == 6
    assert [_page1, _page2] = Enum.slice(stream, 0..1)
  end

  test "inspecting Xandra.PageStream structs", %{conn: conn} do
    page_stream = Xandra.stream_pages!(conn, "SELECT * FROM alphabet", _params = [])

    assert inspect(page_stream) ==
             ~s(#Xandra.PageStream<[query: "SELECT * FROM alphabet", params: [], options: []]>)
  end
end
