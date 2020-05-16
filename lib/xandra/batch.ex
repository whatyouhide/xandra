defmodule Xandra.Batch do
  @moduledoc """
  Represents a batch of simple and/or prepared queries.

  This module provides a data structure that can be used to group queries and
  execute them as a Cassandra `BATCH` query. Batch queries can be executed
  through `Xandra.execute/3` and `Xandra.execute!/3`; see their respective
  documentation for more information.

  Note that the `t/1` type is not documented as it's not meant
  for public use. If you want to use batches in your typespecs, use `t:t/0`.
  """
  alias Xandra.{Prepared, Simple}

  @enforce_keys [:type]
  defstruct @enforce_keys ++
              [queries: [], default_consistency: nil, protocol_module: nil, custom_payload: nil]

  @type type :: :logged | :unlogged | :counter

  @typedoc false
  @type t(type) :: %__MODULE__{
          type: type,
          queries: [Simple.t() | Prepared.t()],
          default_consistency: atom() | nil,
          protocol_module: module() | nil,
          custom_payload: list({String.t(), binary()}) | nil
        }

  @type t() :: t(type)

  @doc """
  Creates a new batch query.

  `type` represents the type of the batch query (`:logged`, `:unlogged`, or
  `:counter`). See the Cassandra documentation for the meaning of these types.

  `custom_payload` can be used to send custom payload along with the request
  (only protocol version 4). See the "Custom Payload" section in `Xandra.execute/4`.

  ## Examples

      batch = Xandra.Batch.new()

  """
  @spec new(type, custom_payload :: list({String.t(), binary()}) | nil) :: t
  def new(type \\ :logged, custom_payload \\ nil) when type in [:logged, :unlogged, :counter] do
    %__MODULE__{type: type, custom_payload: custom_payload}
  end

  @doc """
  Adds a query to the given `batch`.

  `query` has to be either a simple query (statement) or a prepared query. Note
  that parameters have to be added alongside their corresponding query when
  adding a query to a batch. In contrast with functions like `Xandra.execute/4`,
  simple queries in batch queries only support positional parameters and **do
  not** support named parameters; this is a current Cassandra limitation. If a
  map of named parameters is passed alongside a simple query, an `ArgumentError`
  exception is raised. Named parameters are supported with prepared queries.

  ## Examples

      prepared = Xandra.prepare!(conn, "INSERT INTO users (name, age) VALUES (?, ?)")

      batch =
        Xandra.Batch.new()
        |> Xandra.Batch.add(prepared, ["Rick", 60])
        |> Xandra.Batch.add(prepared, ["Morty", 14])
        |> Xandra.Batch.add(prepared, ["Jerry", 35])
        |> Xandra.Batch.add("DELETE FROM users WHERE name = 'Jerry'")

      Xandra.execute!(conn, batch)

  """
  @spec add(t, Xandra.statement() | Prepared.t(), Xandra.values()) :: t
  def add(batch, query, values \\ [])

  def add(%__MODULE__{} = batch, statement, values)
      when is_binary(statement) and is_list(values) do
    add_query(batch, %Simple{statement: statement}, values)
  end

  def add(%__MODULE__{} = batch, %Prepared{} = prepared, values) when is_map(values) do
    add_query(batch, prepared, Prepared.rewrite_named_params_to_positional(prepared, values))
  end

  def add(%__MODULE__{} = batch, %Prepared{} = prepared, values) when is_list(values) do
    add_query(batch, prepared, values)
  end

  def add(%__MODULE__{}, _query, values) when is_map(values) do
    raise ArgumentError,
          "non-prepared statements inside batch queries only support positional " <>
            "parameters (this is a current Cassandra limitation), got: #{inspect(values)}"
  end

  defp add_query(batch, query, values) do
    queries = [%{query | values: values} | batch.queries]
    %{batch | queries: queries}
  end

  defimpl DBConnection.Query do
    alias Xandra.Frame

    def parse(batch, _options) do
      batch
    end

    def encode(batch, nil, options) do
      batch = %{batch | queries: Enum.reverse(batch.queries)}

      Frame.new(:batch, Keyword.take(options, [:compressor, :tracing, :custom_payload]))
      |> batch.protocol_module.encode_request(batch, options)
      |> Frame.encode(batch.protocol_module)
    end

    def decode(batch, %Frame{} = frame, _options) do
      batch.protocol_module.decode_response(frame, batch)
    end

    def describe(batch, _options) do
      batch
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(batch, options) do
      properties = [
        type: batch.type,
        custom_payload: batch.custom_payload,
        queries: format_queries(Enum.reverse(batch.queries))
      ]

      concat(["#Xandra.Batch<", to_doc(properties, options), ">"])
    end

    defp format_queries(queries) do
      Enum.map(queries, fn
        %Simple{statement: statement, values: values} ->
          {statement, values}

        %Prepared{values: values} = prepared ->
          {prepared, values}
      end)
    end
  end
end
