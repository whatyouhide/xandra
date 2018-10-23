# Changelog

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
