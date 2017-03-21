# Changelog

## v0.5.0

- Added support for date, time, smallint, and tinyint types.

## v0.4.3

- Fixed a bug where conditional prepared queries failed to decode.

## v0.4.2

- Fixed a bug with encoding `nil` values in the protocol.

## v0.4.1

- Fixed a bug where negative varint values were decoded incorrectly.

## v0.4.0

- Fixed a bug where prepared queries inside batch queries were not being reprepared in case of "unprepared" errors. Now, prepared queries that fail with "unprepared" errors in batch queries are reprepared before executing the batch again.
- Added support for "retry strategies" (modules that implement `Xandra.RetryStrategy`) to handle retrying of failed queries.

## v0.3.2

- Added support for named params for prepared queries in batches and started raising an explanatory error message if named params are used in simple queries in batches.
- Added `Xandra.run/3` to execute a function with a single Xandra connection checked out from the pool.

## v0.3.1

- Made statement re-preparing happen on the same connection.

## v0.3.0

- Renamed `Xandra.Connection.Error` to `Xandra.ConnectionError`.
- Added support for clustering with random load balancing strategy.
- Fixed the error message for ping failures.
- Fixed a bug where the TCP socket would not be closed in case of failures during connect.

## v0.2.0

- Added support for compression of protocol data (see documentation for the `Xandra` module).
- Added support for the `:serial_consistency` option in `Xandra.execute(!)/3,4`.
- Added support for the `:timestamp` option in `Xandra.execute(!)/4` when executing simple or prepared queries.
- Fixed a bug when repreparing queries that got stale in the cache.
