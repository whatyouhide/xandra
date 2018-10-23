defmodule AtomKeysTest do
  use XandraTest.IntegrationCase, async: true, start_options: [atom_keys: true]

  alias Xandra.{SchemaChange, SetKeyspace, Void}

  test "each possible result and prepared", %{conn: conn, keyspace: keyspace} do
    result = Xandra.execute!(conn, "USE #{keyspace}")
    assert result == %SetKeyspace{keyspace: String.downcase(keyspace)}

    statement = "CREATE TABLE numbers_atom_keys (figure int PRIMARY KEY)"
    result = Xandra.execute!(conn, statement)

    assert result == %SchemaChange{
             effect: "CREATED",
             options: %{
               keyspace: String.downcase(keyspace),
               subject: "numbers_atom_keys"
             },
             target: "TABLE"
           }

    statement = "INSERT INTO numbers_atom_keys (figure) VALUES (?)"
    result = Xandra.execute!(conn, statement, %{figure: {"int", 123}})
    assert result == %Void{}

    statement = "SELECT * FROM numbers_atom_keys WHERE figure = ?"
    result = Xandra.execute!(conn, statement, [{"int", 123}])
    assert Enum.to_list(result) == [%{figure: 123}]

    result = Xandra.execute!(conn, statement, [{"int", 321}])
    assert Enum.to_list(result) == []

    statement = "SELECT * FROM numbers_atom_keys WHERE figure = :figure"
    result = Xandra.execute!(conn, statement, %{"figure" => {"int", 123}})
    assert Enum.to_list(result) == [%{figure: 123}]

    prepared = Xandra.prepare!(conn, statement)
    result = Xandra.execute!(conn, prepared, %{figure: 123})
    assert Enum.to_list(result) == [%{figure: 123}]
  end
end
