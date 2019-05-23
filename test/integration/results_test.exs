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

  describe "SCHEMA_CHANGE result response" do
    # needs `enable_user_defined_functions: true` in cassandra.yml (default: false)
    @describetag :udf
    @describetag protocol_version: :v4

    test "with user defined function", %{conn: conn, keyspace: keyspace} do
      assert {:ok, result} = Xandra.execute(conn, "USE #{keyspace}")
      assert result == %SetKeyspace{keyspace: String.downcase(keyspace)}

      statement =
        "CREATE FUNCTION id (x int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return x;'"

      assert {:ok, result} = Xandra.execute(conn, statement)

      assert result == %SchemaChange{
               effect: "CREATED",
               options: %{arguments: ["int"], keyspace: String.downcase(keyspace), subject: "id"},
               target: "FUNCTION"
             }

      assert {:ok, result} = Xandra.execute(conn, "DROP FUNCTION id")

      assert result === %SchemaChange{
               effect: "DROPPED",
               options: %{arguments: ["int"], keyspace: String.downcase(keyspace), subject: "id"},
               target: "FUNCTION"
             }
    end

    # udf with arity 2 to test string list decoding
    test "with user defined arity 2 function", %{conn: conn, keyspace: keyspace} do
      assert {:ok, result} = Xandra.execute(conn, "USE #{keyspace}")
      assert result == %SetKeyspace{keyspace: String.downcase(keyspace)}

      statement = """
      CREATE FUNCTION plus (x int, y int) RETURNS NULL ON NULL INPUT
      RETURNS int LANGUAGE java AS 'return x + y;'
      """

      assert {:ok, result} = Xandra.execute(conn, statement)

      assert result == %SchemaChange{
               effect: "CREATED",
               options: %{
                 arguments: ["int", "int"],
                 keyspace: String.downcase(keyspace),
                 subject: "plus"
               },
               target: "FUNCTION"
             }

      assert {:ok, result} = Xandra.execute(conn, "DROP FUNCTION plus")

      assert result === %SchemaChange{
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

      sfunc = """
      CREATE FUNCTION totalState ( state int, val int )
      CALLED ON NULL INPUT
      RETURNS int
      LANGUAGE java AS 'return state + val;'
      """

      statement = """
      CREATE AGGREGATE total(int)
      SFUNC totalState
      STYPE int
      INITCOND 0;
      """

      assert {:ok, result} = Xandra.execute(conn, sfunc)
      assert {:ok, result} = Xandra.execute(conn, statement)

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

  test "inspecting Xandra.Page results", %{conn: conn} do
    Xandra.execute!(conn, "CREATE TABLE users (name text PRIMARY KEY)")
    Xandra.execute!(conn, "INSERT INTO users (name) VALUES ('Jeff')")
    %Xandra.Page{} = page = Xandra.execute!(conn, "SELECT * FROM users")
    assert inspect(page) == ~s(#Xandra.Page<[rows: [%{"name" => "Jeff"}], more_pages?: false]>)
  end
end
