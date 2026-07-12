defmodule SchemaAgreementTest do
  use XandraTest.IntegrationCase, async: true

  # See: https://github.com/whatyouhide/xandra/issues/181
  describe ":wait_for_schema_agreement option" do
    test "waits for schema agreement after a schema change", %{conn: conn} do
      statement = "CREATE TABLE users_wait_finite (id int PRIMARY KEY)"

      assert {:ok, %Xandra.SchemaChange{} = schema_change} =
               Xandra.execute(conn, statement, [], wait_for_schema_agreement: 10_000)

      assert schema_change.effect == "CREATED"
      assert schema_change.target == "TABLE"
    end

    test "supports :infinity as the timeout", %{conn: conn} do
      statement = "CREATE TABLE users_wait_infinity (id int PRIMARY KEY)"

      assert {:ok, %Xandra.SchemaChange{}} =
               Xandra.execute(conn, statement, [], wait_for_schema_agreement: :infinity)
    end

    test "has no effect on queries that don't change the schema", %{conn: conn} do
      assert {:ok, %Xandra.Page{}} =
               Xandra.execute(
                 conn,
                 "SELECT * FROM system.local",
                 [],
                 wait_for_schema_agreement: 10_000
               )
    end

    test "has no effect on failed queries", %{conn: conn} do
      assert {:error, %Xandra.Error{}} =
               Xandra.execute(
                 conn,
                 "CREATE TABLE users_bad (id nonexisting_type PRIMARY KEY)",
                 [],
                 wait_for_schema_agreement: 10_000
               )
    end

    test "works on connections started with atom_keys: true",
         %{start_options: start_options, keyspace: keyspace} do
      start_options = Keyword.merge(start_options, atom_keys: true, keyspace: keyspace)
      conn = start_supervised!({Xandra, start_options}, id: :atom_keys_conn)

      statement = "CREATE TABLE users_wait_atom_keys (id int PRIMARY KEY)"

      assert {:ok, %Xandra.SchemaChange{}} =
               Xandra.execute(conn, statement, [], wait_for_schema_agreement: 10_000)
    end

    test "raises on invalid values" do
      assert_raise NimbleOptions.ValidationError, fn ->
        Xandra.execute(:conn_not_used, "SELECT * FROM system.local", [],
          wait_for_schema_agreement: :never
        )
      end
    end
  end
end
