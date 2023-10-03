defmodule Xandra.Prepared do
  @moduledoc """
  A data structure used to internally represent prepared queries.

  See [`%Xandra.Prepared{}`](`__struct__/0`) for information about which fields are
  public. All other fields are documented in `t:t/0` to avoid Dialyzer warnings,
  but are not meant to be used by users.
  """

  @doc """
  A struct that represents a prepared query.

  These are the publicly-accessible fields of this struct:

    * `:tracing_id` - the tracing ID (as a UUID binary) if tracing was enabled,
      or `nil` if no tracing was enabled. See the "Tracing" section in `Xandra.execute/4`.

    * `:response_custom_payload` - the *custom payload* sent by the server, if present.
      If the server doesn't send a custom payload, this field is `nil`. Otherwise,
      it's of type `t:Xandra.custom_payload/0`. See the "Custom payloads" section
      in the documentation for the `Xandra` module.

  """

  alias Xandra.Frame

  defstruct [
    :statement,
    :values,
    :id,
    :bound_columns,
    :result_columns,
    :default_consistency,
    :protocol_module,
    :compressor,
    :tracing_id,
    :request_custom_payload,
    :response_custom_payload,
    :keyspace,
    :result_metadata_id
  ]

  @typedoc """
  The type for a prepared query.

  The only public fields here are `:tracing_id` and `:response_custom_payload`.
  See [`%Xandra.Prepared{}`](`__struct__/0`).
  """
  @type t :: %__MODULE__{
          statement: Xandra.statement(),
          values: Xandra.values() | nil,
          id: binary | nil,
          bound_columns: list | nil,
          result_columns: list | nil,
          default_consistency: atom | nil,
          protocol_module: module | nil,
          compressor: module | nil,
          tracing_id: binary | nil,
          request_custom_payload: Xandra.custom_payload() | nil,
          response_custom_payload: Xandra.custom_payload() | nil,
          keyspace: binary | nil,
          result_metadata_id: binary | nil
        }

  @doc false
  def rewrite_named_params_to_positional(%__MODULE__{} = prepared, params) when is_map(params) do
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

  @doc false
  @spec encode(t(), Xandra.values(), keyword()) :: iodata()
  def encode(prepared, values, options)

  def encode(prepared, values, options) when is_map(values) do
    encode(prepared, rewrite_named_params_to_positional(prepared, values), options)
  end

  def encode(%__MODULE__{} = prepared, values, options) when is_list(values) do
    Frame.new(:execute,
      tracing: options[:tracing],
      compressor: prepared.compressor,
      custom_payload: prepared.request_custom_payload,
      stream_id: Keyword.get(options, :stream_id, 0)
    )
    |> prepared.protocol_module.encode_request(%{prepared | values: values}, options)
    |> Frame.encode(prepared.protocol_module)
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
