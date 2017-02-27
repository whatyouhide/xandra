defmodule Xandra.Prepared do
  @moduledoc """
  A data structure used to internally represent prepared queries.
  """

  defstruct [:statement, :values, :id, :bound_columns, :result_columns]

  @opaque t :: %__MODULE__{
    statement: Xandra.statement,
    values: Xandra.values | nil,
    id: binary | nil,
    bound_columns: list | nil,
    result_columns: list | nil,
  }

  @doc false
  def normalize_params_to_positional(prepared, params)

  def normalize_params_to_positional(%__MODULE__{} = prepared, named_params)
      when is_map(named_params) do
    Enum.map(prepared.bound_columns, fn {_keyspace, _table, name, _type} ->
      case Map.fetch(named_params, name) do
        {:ok, value} ->
          value
        :error ->
          raise ArgumentError,
            "missing named parameter #{inspect(name)} for prepared query #{inspect(prepared)}, " <>
            "got parameters: #{inspect(named_params)}"
      end
    end)
  end

  def normalize_params_to_positional(%__MODULE__{}, positional_params) do
    positional_params
  end

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(prepared, _options) do
      prepared
    end

    def encode(prepared, values, options) do
      values = @for.normalize_params_to_positional(prepared, values)

      Frame.new(:execute)
      |> Protocol.encode_request(%{prepared | values: values}, options)
      |> Frame.encode(options[:compressor])
    end

    def decode(prepared, %Frame{} = frame, _options) do
      Protocol.decode_response(frame, prepared)
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
