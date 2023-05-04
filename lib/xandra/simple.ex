defmodule Xandra.Simple do
  @moduledoc """
  A data structure used internally to represent simple queries.
  """

  defstruct [
    :statement,
    :values,
    :default_consistency,
    :protocol_module,
    :compressor,
    :custom_payload
  ]

  @typedoc """
  The type for a simple query.

  The fields of this are not meant to be used, but are documented to avoid
  Dialyzer warnings.
  """
  @type t :: %__MODULE__{
          statement: Xandra.statement(),
          values: Xandra.values() | nil,
          default_consistency: atom() | nil,
          protocol_module: module() | nil,
          compressor: module() | nil,
          custom_payload: Xandra.custom_payload() | nil
        }

  defimpl DBConnection.Query do
    alias Xandra.Frame

    def parse(query, _options) do
      query
    end

    # "options" are the options given to DBConnection.execute/4.
    def encode(%@for{} = query, values, options) do
      Frame.new(:query,
        tracing: options[:tracing],
        compressor: query.compressor,
        custom_payload: query.custom_payload
      )
      |> query.protocol_module.encode_request(%@for{query | values: values}, options)
      |> Frame.encode(query.protocol_module)
    end

    def decode(_query, response, _options) do
      response
    end

    def describe(query, _options) do
      query
    end
  end
end
