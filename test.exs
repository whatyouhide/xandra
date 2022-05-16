{:ok, conn} = Xandra.start_link(show_sensitive_data_on_connection_error: true)

statement = "SELECT * FROM system.local"

p = Xandra.prepare!(conn, statement)


Xandra.execute!(conn, p)
|> IO.inspect(label: "Query result")
|> Enum.to_list()
|> IO.inspect(label: "tolisted")
