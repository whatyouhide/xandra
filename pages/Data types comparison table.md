# Data types comparison table

This page contains a table that compares the data types used by Cassandra with their counterparts used by Xandra. Each Cassandra data type corresponds to one or more Elixir data types. What Elixir data type is used for a given Cassandra type is often controlled by a formatting option. For example, a `date` value from Cassandra can be returned as a `Date.t()` struct or as an integer representing the number of days since the Unix epoch depending on the `:date_format` option passed to various Xandra functions.

## Comparison table

| Cassandra data type                                         | Elixir data type                                           |
|-------------------------------------------------------------|------------------------------------------------------------|
| `NULL`                                                      | `nil`                                                      |
| `bool`                                                      | `boolean()`                                                |
| `float`, `double`                                           | `float()`                                                  |
| `int`, `bigint`, `smallint`, `varint`, `tinyint`, `counter` | `integer()`                                                |
| `decimal`                                                   | `Decimal.t()`, `{value, scale}`                            |
| `ascii`, `varchar`, `text`                                  | `String.t()`                                               |
| `blob`                                                      | `binary()`                                                 |
| `date`                                                      | `Date.t()`, `integer()` (days from Unix epoch)             |
| `timestamp`                                                 | `DateTime.t()`, `integer()` (milliseconds from Unix epoch) |
| `time`                                                      | `Time.t()`, `integer()` (nanoseconds from midnight)        |
| `list`                                                      | `list()`                                                   |
| `tuple`                                                     | `tuple()`                                                  |
| `set`                                                       | `MapSet.t()`                                               |
| `map`                                                       | `map()`                                                    |
| `uuid`, `timeuuid`                                          | `binary()`                                                 |
