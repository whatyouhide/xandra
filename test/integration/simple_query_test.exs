defmodule SimpleQueryTest do
  use XandraTest.IntegrationCase, async: true

  # Regression for: https://github.com/lexhide/xandra/issues/211
  test "simple query is correctly executed", %{conn: conn} do
    statement = "CREATE TABLE users (first_name text, last_name text PRIMARY KEY)"
    Xandra.execute!(conn, statement)

    statement = "INSERT INTO users (first_name, last_name) VALUES (:first_name, :last_name)"

    {:ok, %Xandra.Void{}} =
      Xandra.execute(conn, statement, %{
        "first_name" => {"text", "Chandler"},
        "last_name" => {"text", "Bing"}
      })
  end
end
