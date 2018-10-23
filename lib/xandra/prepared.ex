defmodule Xandra.Prepared do
  @moduledoc """
  A data structure used to internally represent prepared queries.
  """

  defstruct [:statement, :values, :id, :bound_columns, :result_columns]

  @opaque t :: %__MODULE__{
            statement: Xandra.statement(),
            values: Xandra.values() | nil,
            id: binary | nil,
            bound_columns: list | nil,
            result_columns: list | nil
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
    alias Xandra.{Frame, Protocol}

    def parse(prepared, _options) do
      prepared
    end

    def encode(prepared, values, options) when is_map(values) do
      encode(prepared, @for.rewrite_named_params_to_positional(prepared, values), options)
    end

    def encode(prepared, values, options) when is_list(values) do
      Frame.new(:execute)
      |> Protocol.encode_request(%{prepared | values: values}, options)
      |> Frame.encode(options[:compressor])
    end

    def decode(prepared, %Frame{} = frame, options) do
      Protocol.decode_response(frame, prepared, options)
    end

    def describe(prepared, _options) do
      prepared
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(prepared, options) do
      concat(["#Xandra.Prepared<", to_doc(prepared.statement, options), ">"])
    end
  end
end
