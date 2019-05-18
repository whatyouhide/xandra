# Xandra

[![Build Status](https://travis-ci.org/lexhide/xandra.svg?branch=master)](https://travis-ci.org/lexhide/xandra)
[![Hex.pm](https://img.shields.io/hexpm/v/xandra.svg)](https://hex.pm/packages/xandra)

> Fast, simple, and robust Cassandra driver for Elixir.

![Cover image](http://i.imgur.com/qtbgj00.jpg)

Xandra is a [Cassandra][cassandra] driver built natively in Elixir and focused on speed, simplicity, and robustness.
This driver works exclusively with the Cassandra Query Language v3 (CQL3) and the native protocol (v3, and v4 are currently supported).

## Features

This library is in its early stages when it comes to features, but we're already [successfully using it in production at Forza Football][production-use]. Currently, the supported features are:

  * performing queries (including parameterized queries) on the Cassandra database
  * connection pooling
  * reconnections in case of connection loss
  * prepared queries (including a local cache of prepared queries on a per-connection basis)
  * batch queries
  * page streaming
  * compression
  * clustering (random and priority load balancing for now)
  * customizable retry strategies for failed queries
  * user-defined types
  * authentication
  * SSL encryption

In the future, we plan to add more features, like more load balancing strategies for clustering. See [the documentation][documentation] for detailed explanation of how the supported features work.

## Installation

Add the `:xandra` dependency to your `mix.exs` file:

```elixir
def deps() do
  [{:xandra, "~> 0.10"}]
end
```

Then, run `mix deps.get` in your shell to fetch the new dependency.

## Overview

The documentation is available [on HexDocs][documentation].

Connections or pool of connections can be started with `Xandra.start_link/1`:

```elixir
{:ok, conn} = Xandra.start_link(nodes: ["127.0.0.1:9042"])
```

This connection can be used to perform all operations against the Cassandra server.

Executing simple queries looks like this:

```elixir
statement = "INSERT INTO users (name, postcode) VALUES ('Priam', 67100)"
{:ok, %Xandra.Void{}} = Xandra.execute(conn, statement, _params = [])
```

Preparing and executing a query:

```elixir
with {:ok, prepared} <- Xandra.prepare(conn, "SELECT * FROM users WHERE name = ?"),
     {:ok, %Xandra.Page{}} <- Xandra.execute(conn, prepared, [_name = "Priam"]),
     do: Enum.to_list(page)
```

Xandra supports streaming pages:

```elixir
prepared = Xandra.prepare!(conn, "SELECT * FROM subscriptions WHERE topic = :topic")
page_stream = Xandra.stream_pages!(conn, prepared, _params = [], page_size: 1_000)

# This is going to execute the prepared query every time a new page is needed:
page_stream
|> Enum.take(10)
|> Enum.each(fn page -> IO.puts("Got a bunch of rows: #{inspect(Enum.to_list(page))}") end)
```

## Scylla support

Xandra supports [Scylla][scylladb] (version `2.x`) without the need to do anything in particular.

## Contributing

To run tests, you will need [Docker][docker] installed on your machine. This repository uses [`docker-compose`][docker-compose] to run multiple Cassandra instances in parallel on different ports to test different features (such as authentication or SSL encryption). To run normal tests, do this from the root of the project:

```bash
docker-compose up -d
mix test
```

The `-d` flags runs the instances as daemons in the background. Give it a minute between starting the services and running `mix test` since Cassandra takes a while to start. To stop the services, run `docker-compose stop`.

To run tests for Scylla, you'll need a different set of services and a different test task:

```bash
docker-compose --file docker-compose.scylladb.yml up -d
mix test.scylladb
```

Use `docker-compose --file docker-compose.scylladb.yml stop` to stop Scylla when done.

## License

Xandra is released under the ISC license, see the [LICENSE](LICENSE) file.

[documentation]: https://hexdocs.pm/xandra
[cassandra]: http://cassandra.apache.org
[production-use]: http://tech.forzafootball.com/blog/the-pursuit-of-instant-pushes
[docker]: https://www.docker.com
[docker-compose]: https://docs.docker.com/compose/
[scylladb]: https://www.scylladb.com/
