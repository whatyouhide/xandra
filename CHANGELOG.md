# Changelog

## v0.19.3

  * Add support for the `ANY` consistency level.
  * Fix connection crash when a newly-added node would send a `UP` message.
  * Fix leaking stream IDs on protocol encoding errors.

## v0.19.2

  * Fix an issue where Xandra would not reconnect to any node if *all* nodes went offline.

## v0.19.1

### Bug Fixes

  * Add missing error `:reason` to `:execute_query` Telemetry events.
  * Fix decoding multiple frames in a single envelope for native protocol v5. This resulted in frequent timeouts when using native protocol v5, which are extensively researched in [this issue](https://github.com/whatyouhide/xandra/issues/356). Well, that's fixed!

## v0.19.0

### Features

  * Add `:max_concurrent_requests_per_connection` option when starting single and cluster connections.

### Bug Fixes

  * Fix a memory leak in `Xandra.Connection`.
  * Raise on unknown options when starting `Xandra.Cluster`.
  * Increase the default TCP/SSL buffer to ~1Mb and make it configurable. This should reduce the number of possible request timeouts across connections.
  * Fix an error that would happen when checking out connections from a cluster and those connections going *down* right after being checked out.

## v0.18.1

### Bug Fixes

  * Fix a memory leak that would happen especially when preparing queries. We recommend everyone to update if you're on 0.18.0, since this is a low-risk release with minimal changes.

## v0.18.0

This is a **big release**. 🚀

The main deal here is that we removed the [`db_connection`](https://github.com/elixir-ecto/db_connection) underlying library, and completely **rewrote** Xandra's internal connection to be able to make *concurrent requests*. [This issue](https://github.com/lexhide/xandra/issues/333) has a little more context around this decision. To make these changes possible, we also had to require Elixir v1.15 or later.

This change is breaking, and affects Xandra pretty significantly. The **user-facing changes** are:

  * `Xandra` doesn't start a pool of connections anymore, only a single connection. Pooling is on you. We recommend [registry-based pooling](https://andrealeopardi.com/posts/process-pools-with-elixirs-registry/) if you want to keep the single-node aspect of `Xandra`. Alternatively, use `Xandra.Cluster` with a single node (`Xandra.Cluster` supports pooling). We might eventually add built-in registry-based pooling, but we're evaluating it.

  * `Xandra.start_link/1` doesn't support *all* `DBConnection.start_link/2` options anymore. Make sure to refer to the updated documentation for `Xandra.start_link/1` to know the exact supported options.

  * `Xandra.execute/2,3,4` and `Xandra.prepare/2,3` don't support *all* `DBConnection` options anymore. Check their documentation for more information.

Below is a detailed list of all the changes.

### Breaking Changes

  * The `Xandra` module doesn't support pooling anymore and is now only a single connection.
  * Removed the `Xandra.db_connection_start_option/0` type.
  * Removed the `Xandra.xandra_start_option/0` type.
  * Removed some `DBConnection.start_link/2` options in `Xandra.start_link/1`.
  * Removed the `:pool_size` option in `Xandra.start_link/1`.
  * Require Elixir 1.15+.

### Improvements and Bug Fixes

  * Make retry strategies **cluster aware**, by adding the `{:retry, new_options, new_state, conn_pid}` return value to the `retry/3` callback. See the updated documentation for `Xandra.RetryStrategy`.
  * Support `GenServer.start_link/3` options in `Xandra.Cluster.start_link/1` (like `:spawn_opt` and friends).
  * Add the `:queue_checkouts_before_connecting` option to `Xandra.Cluster.start_link/1` to queue checkout requests in the cluster until at least one connection to one node is established. This is intended to mostly replace `:sync_connect`.
  * Fix a few bugs with rogue data in the native protocol parser.
  * Fix a small bug when negotiating the native protocol version.
  * Fix IPv6 support in `Xandra.Cluster`.
  * Add the `:connect_timeout` option to `Xandra.start_link/1` and `Xandra.Cluster.start_link/1`.
  * Add the `Xandra.native_protocol_version/0` type.
  * Add the `Xandra.consistency/0` type.
  * Improve validation of start options.

## v0.17.0

  * Move most logs to Telemetry events instead. If you want to keep the same behavior as before, call this in your application's `start/2` callback:

    ```elixir
    Xandra.Telemetry.attach_default_handler()
    ```

    See the `Xandra.Telemetry` documentation for more information. This was started in v0.15.0, but now most logs have been moved to Telemetry.

## v0.16.0

  * Add the `:sync_connect` option to `Xandra.Cluster.start_link/1`.

## v0.15.0

This is a bigger release than usual. Part of it is that the previous release happened quite a long time before this one.

The main changes in this release are all around **clustering**. We made Xandra's clustering significantly smarter. `Xandra.Cluster` now:

  * autodiscovers nodes in the cluster by default
  * uses less resources (only one *control connection* rather than one per node)
  * has support for "load-balancing policies" to control which nodes to connect to and which nodes to route queries too

This is also the first release where Xandra starts publishing [Telemetry](https://github.com/beam-telemetry/telemetry) events.

**Breaking changes**:

  * Drop support for Cassandra version 2 and older.
  * Drop support for Elixir 1.10 and below. Xandra now requires Elixir 1.11+.
  * Drop support for nimble_options `0.x`. Xandra now depends on nimble_options `~> 1.0`.
  * Remove the `:priority` value for the `:load_balancing` option in `Xandra.Cluster.start_link/1`.
  * Always enable node autodiscovery for clusters.
  * Remove support for the `:address` and `:port` options in `Xandra.start_link/1`.

**Improvements**:

  * Add support for the Cassandra *native protocol* v5 (previously supported were only v3 and v4).
  * Start emitting [Telemetry](https://github.com/beam-telemetry/telemetry) events, see `Xandra.Telemetry`.
  * Add support for **custom payloads**.
  * Add support for Decimal 2.0+ (and keep supporting 1.x).
  * Add the following options to `Xandra.Cluster.start_link/1`:
    * `:refresh_topology_interval`
    * `:target_pools`
  * Add the `Xandra.Cluster.LoadBalancingPolicy` behaviour and the following built-in load-balancing policies:
    * `Xandra.Cluster.LoadBalancingPolicy.Random`
    * `Xandra.Cluster.LoadBalancingPolicy.DCAwareRoundRobin`
  * Add `Xandra.start_link_opts_schema/0`.
  * Add `Xandra.Cluster.Host`.
  * Add `Xandra.stop/1-3` and `Xandra.Cluster.stop/1-3`.
  * Make the `Xandra.conn/0` type public.

**Bug fixes and deprecations**:

  * Deprecate the `:autodiscovery` option in `Xandra.Cluster.start_link/1`.
  * Deprecate the `:random` value for the `:load_balancing` option in `Xandra.Cluster.start_link/1`.
  * Fix spec for the `Xandra.Batch.t/0` type.
  * Add some missing error codes to the native protocols.
  * Fix a bug where we were not using the `:default_consistency` option in `Xandra.start_link/1`.

## v0.14.0

**Breaking changes**:

  * Drop support for Elixir 1.8 and below. Xandra now requires Elixir 1.9+.

**Improvements**:

  * Implement significantly-more accurate and strict validation of options for `Xandra` functions.

**Bug fixes**:

  * Fix schema change payloads with UDFs in native protocol v3.
  * Filter nodes with null `host_id` when autodiscovering nodes in `Xandra.Cluster`.
  * Fix some warnings related to upgrading DBConnection.
  * Make connections aware of the current keyspace, which fixes some issues with the prepared query cache.
  * Fix an issue where pagination would break for some batch queries: with a lightweight transaction the returned result would be a page representing the status of that transaction, and we did not support that.

## v0.13.1

  * Fix the spec for `Xandra.Batch.add/3`.
  * Reconnect to the control connection using the full address (and not the IP of the peername) in `Xandra.Cluster`.
  * Set the `:xandra_address` and `:xandra_port` metadata in logs for Xandra connections.

## v0.13.0

  * Add support for Cassandra native protocol v4. By default, we'll use v3, but you can pass in a `:protocol_version` when starting a connection to force protocol v4. Protocol v4 introduces the `:not_set` value. See the documentation of the `Xandra` module.
  * Add support for tracing on a per-query basis. If `:tracing` is set to `true`, then a tracing ID will be returned with the Cassandra response. See the documentation for more information.
  * Fix a bug with decoding `Decimal` values.
  * Fix a bug with `Xandra.Cluster` where, in cases of clusters with a single node, the connection wouldn't reconnect to the node in case of disconnections.

**Breaking changes**:

  * Remove `Xandra.Page.more_pages_available?/1`. This was hard-deprecated in previous versions.
  * Remove the `:cursor` option for queries. This was hard-deprecated in previous versions.

## v0.12.0

  * Add a page in the documentation that compares Elixir data types and their Cassandra counterparts.
  * Fix some Dialyzer errors caused by opaque data types.
  * Support cluster-aware retrying through `:retry_strategy` in `Xandra.Cluster.execute/3,4` (in the previous release we would retry queries but only on the same node).
  * Fix `Xandra.Cluster.stream_pages!/4` which was broken in the previous release.

**Breaking changes**:

  * Change the format of `inet` values for IPv6. When encoding a IPv6 address, it should be given to Xandra as an 8-element tuple of integers representing byte couples. When Cassandra returns IPv6 addresses, they are now returned as 8-element tuples of integers representing byte couples. This is to align Xandra with the usage of IPv6 addresses in Erlang/OTP (see the `:inet.ip_address/0` type).
  * Support autodiscovery of nodes in the same data center in `Xandra.Cluster` with support for the `:random` load balancing strategy only. This also means support for nodes that are added or removed to the cluster (provided they're in the same datacenter). This is a breaking change because autodiscovery is active **by default**. If you want to keep the previous behavior, pass `autodiscovery: false` to `Xandra.Cluster.start_link/1`.

## v0.11.0

  * Add `Xandra.child_spec/1`.
  * Add encryption support through the `:encryption` option.
  * Add the `:decimal_format` option to return decimals as tuples or [`Decimal`](https://github.com/ericmj/decimal) structs. If you want to use `decimal_format: :decimal`, you have to specify [decimal](https://github.com/ericmj/decimal) as a dependency.
  * Add the `:default_consistency` option to provide a connection-wide default consistency.

**Breaking changes**:

  * Add the `:uuid_format` and `:timeuuid_format` options to return UUIDs as binaries or human-readable strings. This is a breaking change because the default changed to `:string`. If you want to keep the previous behavior, pass `uuid_format: :binary` or `timeuuid_format: :binary` to `Xandra.execute/3/4`.
  * Remove support for the `:pool` option in Xandra. Now the pool of connections is always a pool with size configurable by `:pool_size`.
  * Add `Xandra.Cluster` as a separate module with an API that mirrors `Xandra`, instead of as a DBConnection pool.
  * Bump the Elixir requirement to ~> 1.6.

## v0.10.0

  * Added the `:atom_keys` option to return and accept column names as atoms.
  * Fixed decoding of user-defined data types when new fields added afterwards.
  * Fixed connection ping failures when using compression.

## v0.9.2

  * Fixed Elixir v1.6 warnings.

## v0.9.1

  * Started accepting UUIDs in binary representation.
  * Added handling of legacy empty values for non-string types.

## v0.9.0

  * Added native support for `Date`, `Time`, and `DateTime`.
  * Added support for the counter data type.
  * Made more optimizations to page content decoding.
  * Replaced the `:cursor` option used for manual result paging with the more explicit `:paging_state` option.

**Breaking changes**:

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

  * Fixed a bug where prepared queries inside batch queries were not being re-prepared in case of "unprepared" errors. Now, prepared queries that fail with "unprepared" errors in batch queries are re-prepared before executing the batch again.
  * Added support for "retry strategies" (modules that implement `Xandra.RetryStrategy`) to handle retrying of failed queries.

## v0.3.2

  * Added support for named parameters for prepared queries in batches and started raising an explanatory error message if named parameters are used in simple queries in batches.
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
