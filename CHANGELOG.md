# Changelog

## v0.14.0

* Add support for custom payload as defined in protocol version 4. This adds support for Azure Cosmos DB.

## v0.13.1

* Fix the spec for `Xandra.Batch.add/3`.
* Reconnect to the control connection using the full address (and not the IP of the peername) in `Xandra.Cluster`.
* Set the `:xandra_address` and `:xandra_port` metadata in logs for Xandra connections.

## v0.13.0

* Add support for Cassandra native protocol v4. By default, we'll use v3, but you can pass in a `:protocol_version` when starting a connection to force protocol v4. Protocol v4 introduces the `:not_set` value. See the documentation of the `Xandra` module.
* Add support for tracing on a per-query basis. If `:tracing` is set to `true`, then a tracing ID will be returned with the Cassandra response. See the documentation for more information.
* Fix a bug with decoding `Decimal` values.
* Fix a bug with `Xandra.Cluster` where, in cases of clusters with a single node, the connection wouldn't reconnect to the node in case of disconnections.

__Breaking changes__:

* Remove `Xandra.Page.more_pages_available?/1`. This was hard-deprecated in previous versions.
* Remove the `:cursor` option for queries. This was hard-deprecated in previous versions.


## v0.12.0

* Add a page in the documentation that compares Elixir data types and their Cassandra counterparts.
* Fix some Dialyzer errors caused by opaque data types.
* Support cluster-aware retrying through `:retry_strategy` in `Xandra.Cluster.execute/3,4` (in the previous release we would retry queries but only on the same node).
* Fix `Xandra.Cluster.stream_pages!/4` which was broken in the previous release.

__Breaking changes__:

* Change the format of `inet` values for IPv6. When encoding a IPv6 address, it should be given to Xandra as a 8-element tuple of integers representing byte couples. When IPv6 addresses are returned from Cassandra, they are now returned as 8-element tuples of integers representing byte couples. This is to align Xandra with the usage of IPv6 addresses in Erlang/OTP (see the `:inet.ip_address/0` type).
* Support autodiscovery of nodes in the same data center in `Xandra.Cluster` with support for the `:random` load balancing strategy only. This also means support for nodes that are added or removed to the cluster (provided they're in the same datacenter). This is a breaking change because autodiscovery is active **by default**. If you want to keep the previous behaviour, pass `autodiscovery: false` to `Xandra.Cluster.start_link/1`.

## v0.11.0

* Add `Xandra.child_spec/1`.
* Add encryption support through the `:encryption` option.
* Add the `:decimal_format` option to return decimals as tuples or [`Decimal`](https://github.com/ericmj/decimal) structs. If you want to use `decimal_format: :decimal` you have to specify [decimal](https://github.com/ericmj/decimal) as a dependency.
* Add the `:default_consistency` option to provide a connection-wide default consistency.

__Breaking changes__:

* Add the `:uuid_format` and `:timeuuid_format` options to return UUIDs as binaries or human-readable strings. This is a breaking change because the default changed to `:string`. If you want to keep the previous behaviour, pass `uuid_format: :binary` or `timeuuid_format: :binary` to `Xandra.execute/3/4`.
* Remove support for the `:pool` option in Xandra. Now the pool of connections is always a pool with size configurable by `:pool_size`.
* Add `Xandra.Cluster` as a separate module with an API that mirrors `Xandra`, instead of as a DBConnection pool.
* Bump the Elixir requirement to ~> 1.6.

## v0.10.0

* Added the `:atom_keys` option to return and accept column names as atoms.
* Fixed decoding of user-defined data types when new fields added afterwards.
* Fixed connection ping failures when compression is used.

## v0.9.2

* Fixed Elixir v1.6 warnings.

## v0.9.1

* Started accepting UUIDs in binary representation.
* Added handling of legacy empty values for non-string types.

## v0.9.0

* Added native support for `Date`, `Time`, and `DateTime`.
* Added support for the counter data type.
* Made more optimizations to page content decoding.
* Replaced the `:cursor` option used for manaul result paging with the more explicit `:paging_state` option.

__Breaking changes:__

* Started decoding by default the date, time, and timestamp data types to `Date`, `Time`, and `DateTime` respectively.

## v0.8.0

* Added support for priority load balancing strategy for clustering.

## v0.7.2

* Fixed a bug in the binary protocol that affected nodes going up/down when using `Xandra.Cluster`.

## v0.7.1

* Made `Xandra.Cluster` to fully utilize authentication.

## v0.7.0

* Added support for authentication.

## v0.6.1

* Optimized page content decoding by using single match context.

## v0.6.0

* Added support for user-defined data types.

## v0.5.1

* Optimized request encoding by using iodata.

## v0.5.0

* Added support for date, time, smallint, and tinyint data types.

## v0.4.3

* Fixed a bug where conditional prepared queries failed to decode.

## v0.4.2

* Fixed a bug with encoding `nil` values in the protocol.

## v0.4.1

* Fixed a bug where negative varint values were decoded incorrectly.

## v0.4.0

* Fixed a bug where prepared queries inside batch queries were not being reprepared in case of "unprepared" errors. Now, prepared queries that fail with "unprepared" errors in batch queries are reprepared before executing the batch again.
* Added support for "retry strategies" (modules that implement `Xandra.RetryStrategy`) to handle retrying of failed queries.

## v0.3.2

* Added support for named params for prepared queries in batches and started raising an explanatory error message if named params are used in simple queries in batches.
* Added `Xandra.run/3` to execute a function with a single Xandra connection checked out from the pool.

## v0.3.1

* Made statement re-preparing happen on the same connection.

## v0.3.0

* Renamed `Xandra.Connection.Error` to `Xandra.ConnectionError`.
* Added support for clustering with random load balancing strategy.
* Fixed the error message for ping failures.
* Fixed a bug where the TCP socket would not be closed in case of failures during connect.

## v0.2.0

* Added support for compression of protocol data (see documentation for the `Xandra` module).
* Added support for the `:serial_consistency` option in `Xandra.execute(!)/3,4`.
* Added support for the `:timestamp` option in `Xandra.execute(!)/4` when executing simple or prepared queries.
* Fixed a bug when repreparing queries that got stale in the cache.
