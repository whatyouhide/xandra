defmodule AnyConsistencyTest do
  use XandraTest.IntegrationCase, async: true

  # Regression for: https://github.com/lexhide/xandra/issues/381
  test "queries with :any consistency are correctly executed", %{conn: conn} do
    statement = "CREATE TABLE cyclists (first_name text, last_name text PRIMARY KEY)"
    Xandra.execute!(conn, statement, %{}, consistency: :any)

    statement = "INSERT INTO cyclists (first_name, last_name) VALUES (:first_name, :last_name)"

    {:ok, %Xandra.Void{}} =
      Xandra.execute(
        conn,
        statement,
        %{
          "first_name" => {"text", "KRUIKSWIJK"},
          "last_name" => {"text", "Steven"}
        },
        consistency: :any
      )
  end
end
