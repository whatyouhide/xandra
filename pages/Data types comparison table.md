# Data Types Comparison Table

This page contains a table that compares the data types used by Cassandra with their counterparts used by Xandra. Each Cassandra data type corresponds to one or more Elixir data types. What Elixir data type is used for a given Cassandra type is often controlled by a formatting option. For example, a `date` value from Cassandra can be returned as a `Date.t()` struct or as an integer representing the number of days since the Unix epoch depending on the `:date_format` option passed to various Xandra functions.

## Comparison Table

| Cassandra data type | Elixir data type                                                                 |
| ------------------- | -------------------------------------------------------------------------------- |
| `ascii`             | `t:String.t/0`                                                                   |
| `bigint`            | `t:integer/0`                                                                    |
| `blob`              | `t:binary/0`                                                                     |
| `boolean`           | `t:boolean/0`                                                                    |
| `counter`           | `t:integer/0`                                                                    |
| `date`              | `t:Date.t/0` (if `date_format: :date`)                                           |
| `date`              | `t:integer/0` (if `date_format: :integer`), days from Unix epoch                 |
| `decimal`           | `{value, scale}` (if `decimal_format: :tuple`), where `value * 10^(-1 * scale)`  |
| `decimal`           | `t:Decimal.t/0` (if `decimal_format: :decimal`)                                  |
| `double`            | `t:float/0`                                                                      |
| `float`             | `t:float/0`                                                                      |
| `inet`              | `t::inet.ip_address/0`                                                           |
| `int`               | `t:integer/0`                                                                    |
| `list<T>`           | `t:list/0`                                                                       |
| `map<KeyT, ValueT>` | `t:map/0`                                                                        |
| `NULL`              | `nil`                                                                            |
| `set<T>`            | `t:MapSet.t/0`                                                                   |
| `smallint`          | `t:integer/0`                                                                    |
| `text`              | `t:String.t/0`                                                                   |
| `time`              | `t:integer/0` (if `time_format: :integer`), as nanoseconds from midnight         |
| `time`              | `t:Time.t/0` (if `time_format: :time`)                                           |
| `timestamp`         | `t:DateTime.t/0` (if `timestamp_format: :datetime`)                              |
| `timestamp`         | `t:integer/0` (if `timestamp_format: :integer`), as milliseconds from Unix epoch |
| `timeuuid`          | `t:binary/0`                                                                     |
| `tinyint`           | `t:integer/0`                                                                    |
| `tuple<...>`        | `t:tuple/0`                                                                      |
| `uuid`              | `t:binary/0`                                                                     |
| `varchar`           | `t:String.t/0`                                                                   |
| `varint`            | `t:integer/0`                                                                    |
