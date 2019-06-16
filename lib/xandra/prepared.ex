defmodule Xandra.Prepared do
  @moduledoc """
  A data structure used to internally represent prepared queries.

  Note that the `t:t/0` type is public because it would cause Dialyzer
  warnings if it were opaque. However, the `Xandra.Prepared` struct is
  private API and is not meant to be used directly.
  """

  defstruct [
    :statement,
    :values,
    :id,
    :bound_columns,
    :result_columns,
    :default_consistency,
    :protocol_module,
    :tracing_id
  ]

  @type t :: %__MODULE__{
          statement: Xandra.statement(),
          values: Xandra.values() | nil,
          id: binary | nil,
          bound_columns: list | nil,
          result_columns: list | nil,
          default_consistency: atom | nil,
          protocol_module: module | nil,
          tracing_id: binary | nil
        }

  @doc false
  def rewrite_named_params_to_positional(%__MODULE__{} = prepared, params)
      when is_map(params) do
    Enum.map(prepared.bound_columns, fn {_keyspace, _table, name, _type} ->
      case Map.fetch(params, name) do
        {:ok, value} ->
          value

        :error ->
          raise ArgumentError,
                "missing named parameter #{inspect(name)} for prepared query, " <>
                  "got: #{inspect(params)}"
      end
    end)
  end

  defimpl DBConnection.Query do
    alias Xandra.Frame

    def parse(prepared, _options) do
      prepared
    end

    def encode(prepared, values, options) when is_map(values) do
      encode(prepared, @for.rewrite_named_params_to_positional(prepared, values), options)
    end

    def encode(prepared, values, options) when is_list(values) do
      Frame.new(:execute, Keyword.take(options, [:compressor]))
      |> prepared.protocol_module.encode_request(%{prepared | values: values}, options)
      |> Frame.encode(prepared.protocol_module)
    end

    def decode(prepared, %Frame{} = frame, options) do
      prepared.protocol_module.decode_response(frame, prepared, options)
    end

    def describe(prepared, _options) do
      prepared
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(prepared, options) do
      properties = [
        statement: prepared.statement,
        tracing_id: prepared.tracing_id
      ]

      concat(["#Xandra.Prepared<", to_doc(properties, options), ">"])
    end
  end
end
