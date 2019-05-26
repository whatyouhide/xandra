# Data types comparison table

This page contains a table that compares the datatypes used by Cassandra with their counterparts used by Xandra. Each Cassandra datatype corresponds to one or more Elixir datatypes. What Elixir datatype is used for a given Cassandra type is often controlled by a formatting option. For example, a `date` value from Cassandra can be returned as a `Date.t()` struct or as an integer representing the number of days since the Unix epoch depending on the `:date_format` option passed to various Xandra functions.

## Comparison table

| Cassandra type                                              | Elixir type                                                |
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
