# Xandra

> Fast, simple, and robust Cassandra driver for Elixir.

Xandra is a [Cassandra][cassandra] driver built natively in Elixir focused on speed, simplicity, and robustness.

## Features

This library is in its early stages when it comes to features, but we're already successfully using it in production at [Football Addicts][football-addicts]. Currently, the supported features are:

  * performing queries (including parameterized queries) on the Cassandra database
  * connection pooling
  * reconnections in case of connection loss
  * prepared queries (including a local cache of prepared queries on a per-connection basis)
  * batch queries
  * page streaming

In the future, we plan to add several more features, like clustering support or query retrying strategies. See [the documentation][documentation] for detailed explanation of how these features work.

## Installation

Add the `:xandra` dependency to your `mix.exs` file:

```elixir
def deps() do
  [{:xandra, ">= 0.0.0"}]
end
```

and add `:xandra` to your list of applications:

```elixir
def application() do
  [applications: [:logger, :xandra]]
end
```

Then, run `mix deps.get` in your shell to fetch the new dependency.

## Overview

The documentation is available [on HexDocs][documentation].

Connections or pool of connections can be started with `Xandra.start_link/1`:

```elixir
{:ok, conn} = Xandra.start_link(host: "127.0.0.1", port: 9042)
```

This connection can be used to perform all operations against the Cassandra server.

Executing simple queries looks like this:

```elixir
statement = "INSERT INTO users (name, email) VALUES ('Jeff', 'jeff@community.com')"
{:ok, %Xandra.Void{}} = Xandra.execute(conn, statement, _params = [])
```

Preparing and executing a query:

```elixir
with {:ok, prepared} <- Xandra.prepare(conn, "SELECT * FROM users WHERE name = ?"),
     {:ok, %Xandra.Page{}} <- Xandra.execute(conn, prepared, [_name = "Jeff"]),
     do: Enum.to_list(page)
```

Xandra supports streaming pages:

```elixir
prepared = Xandra.prepare!(conn, "SELECT * FROM subscriptions WHERE topic = :topic")
pages_stream = Xandra.stream_pages!(conn, prepared, _params = [], page_size: 1_000)

# This is going to execute the prepared query every time a new page is needed
pages_stream
|> Enum.take(10)
|> Enum.each(fn(page) -> IO.puts "Got a bunch of rows: #{inspect(Enum.to_list(page))}" end)
```

## Contributing

Clone the repository and run `$ mix test` to make sure your setup is correct; you're going to need Cassandra running locally on its default port (`9042`) for tests to pass.

## License

ISC &copy; 2017 Aleksei Magusev and Andrea Leopardi, see the [license file](LICENSE).

[documentation]: https://hexdocs.pm/xandra
[cassandra]: http://cassandra.apache.org
[football-addicts]: https://www.footballaddicts.com
