defmodule ResultsTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.{SchemaChange, SetKeyspace, Void}

  @tag :cassandra_specific
  test "each possible result", %{conn: conn, keyspace: keyspace} do
    assert {:ok, result} = Xandra.execute(conn, "USE #{keyspace}")
    assert result == %SetKeyspace{keyspace: String.downcase(keyspace)}

    statement = "CREATE TABLE numbers (figure int PRIMARY KEY)"
    assert {:ok, result} = Xandra.execute(conn, statement)

    assert result == %SchemaChange{
             effect: "CREATED",
             options: %{
               keyspace: String.downcase(keyspace),
               subject: "numbers"
             },
             target: "TABLE"
           }

    statement = "INSERT INTO numbers (figure) VALUES (123)"
    assert {:ok, result} = Xandra.execute(conn, statement)
    assert result == %Void{}

    statement = "SELECT * FROM numbers WHERE figure = ?"
    assert {:ok, result} = Xandra.execute(conn, statement, [{"int", 123}])
    assert Enum.to_list(result) == [%{"figure" => 123}]

    assert {:ok, result} = Xandra.execute(conn, statement, [{"int", 321}])
    assert Enum.to_list(result) == []

    statement = "SELECT * FROM numbers WHERE figure = :figure"
    assert {:ok, result} = Xandra.execute(conn, statement, %{"figure" => {"int", 123}})
    assert Enum.to_list(result) == [%{"figure" => 123}]
  end

  test "inspecting Xandra.Page results", %{conn: conn, is_cosmosdb: is_cosmosdb} do
    Xandra.execute!(conn, "CREATE TABLE users (name text PRIMARY KEY)")
    Xandra.execute!(conn, "INSERT INTO users (name) VALUES ('Jeff')")
    %Xandra.Page{} = page = Xandra.execute!(conn, "SELECT * FROM users")

    expected =
      ~s(#Xandra.Page<[rows: [%{"name" => "Jeff"}], tracing_id: nil, custom_payload: ) <>
        if is_cosmosdb do
          ~s([{"RequestCharge", <<64, 5, 27, 243, 217, 33, 92, 197>>}])
        else
          "nil"
        end <>
        ~s(, more_pages?: false]>)

    assert inspect(page) == expected
  end

  describe "SCHEMA_CHANGE updates since native protocol v4" do
    @describetag :cassandra_specific

    setup %{start_options: start_options} do
      start_options = Keyword.put(start_options, :protocol_version, :v4)
      {:ok, conn} = Xandra.start_link(start_options)
      %{conn: conn}
    end

    test "user defined function", %{conn: conn, keyspace: keyspace} do
      assert {:ok, result} = Xandra.execute(conn, "USE #{keyspace}")
      assert result == %SetKeyspace{keyspace: String.downcase(keyspace)}

      statement = """
      CREATE FUNCTION plus (x int, y int)
      RETURNS NULL ON NULL INPUT
      RETURNS int
      LANGUAGE java AS 'return x;'
      """

      assert Xandra.execute!(conn, statement) == %SchemaChange{
               effect: "CREATED",
               options: %{
                 arguments: ["int", "int"],
                 keyspace: String.downcase(keyspace),
                 subject: "plus"
               },
               target: "FUNCTION"
             }

      statement = "DROP FUNCTION plus"

      assert Xandra.execute!(conn, statement) === %SchemaChange{
               effect: "DROPPED",
               options: %{
                 arguments: ["int", "int"],
                 keyspace: String.downcase(keyspace),
                 subject: "plus"
               },
               target: "FUNCTION"
             }
    end

    test "with user defined aggregate", %{conn: conn, keyspace: keyspace} do
      assert {:ok, result} = Xandra.execute(conn, "USE #{keyspace}")
      assert result == %SetKeyspace{keyspace: String.downcase(keyspace)}

      Xandra.execute!(conn, """
      CREATE FUNCTION totalState (state int, val int)
      CALLED ON NULL INPUT
      RETURNS int
      LANGUAGE java AS 'return state + val;'
      """)

      result =
        Xandra.execute!(conn, """
        CREATE AGGREGATE total(int)
        SFUNC totalState
        STYPE int
        INITCOND 0;
        """)

      assert result == %SchemaChange{
               effect: "CREATED",
               options: %{
                 arguments: ["int"],
                 keyspace: String.downcase(keyspace),
                 subject: "total"
               },
               target: "AGGREGATE"
             }
    end
  end
end
