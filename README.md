# Xandra

[![hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/xandra)
[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)][documentation]
[![CI](https://github.com/lexhide/xandra/actions/workflows/main.yml/badge.svg)](https://github.com/lexhide/xandra/actions/workflows/main.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/xandra.svg)](https://hex.pm/packages/xandra)

> Fast and robust Cassandra driver for Elixir.

![Cover image](http://i.imgur.com/qtbgj00.jpg)

Xandra is a [Cassandra][cassandra] driver built natively in Elixir and focused
on speed, simplicity, and robustness. This driver works exclusively with the
Cassandra Query Language v3 (CQL3) and native protocol *v3*, *v4*, and *v5*.

## Features

  * 🎱 Connection pooling with automatic reconnections
  * 🌯 Prepared queries (with local cache of prepared queries on a
    per-connection basis) and batch queries
  * 📃 Page streaming
  * 🗜️ LZ4 compression
  * 👩‍👩‍👧‍👧 Clustering with support for autodiscovery of nodes in the cluster
  * 🔁 Customizable retry strategies for failed queries
  * 👩‍💻 User-defined types
  * 🔑 Authentication
  * 🔐 SSL encryption

See [the documentation][documentation] for detailed explanation of how the
supported features work.

## Installation

Add the `:xandra` dependency to your `mix.exs` file:

```elixir
defp deps do
  [
    {:xandra, "~> 0.19"}
  ]
end
```

Then, run `mix deps.get` in your shell to fetch the new dependency.

## Overview

The documentation is available [on HexDocs][documentation].

Connections or pool of connections can be started with `Xandra.start_link/1`:

```elixir
{:ok, conn} = Xandra.start_link(nodes: ["127.0.0.1:9042"])
```

This connection can be used to perform all operations against the Cassandra
server. Executing simple queries looks like this:

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

Xandra supports [Scylla][scylladb] (version `2.x` to `5.x`) without the need to do anything in particular.

## License

Xandra is released under the ISC license, see the [LICENSE](LICENSE) file.

[documentation]: https://hexdocs.pm/xandra
[cassandra]: http://cassandra.apache.org
[scylladb]: https://www.scylladb.com/
