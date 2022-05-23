defmodule Xandra.Simple do
  @moduledoc false

  defstruct [:statement, :values, :default_consistency, :protocol_module, :compressor]

  @type t :: %__MODULE__{
          statement: Xandra.statement(),
          values: Xandra.values() | nil,
          default_consistency: atom() | nil,
          protocol_module: module() | nil,
          compressor: module() | nil
        }

  defimpl DBConnection.Query do
    alias Xandra.Frame

    def parse(query, _options) do
      query
    end

    # "options" are the options given to DBConnection.execute/4.
    def encode(%@for{} = query, values, options) do
      Frame.new(:query, tracing: options[:tracing], compressor: query.compressor)
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
