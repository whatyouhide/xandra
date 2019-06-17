defmodule Xandra.Simple do
  @moduledoc false

  defstruct [:statement, :values, :default_consistency, :protocol_module]

  @type t :: %__MODULE__{
          statement: Xandra.statement(),
          values: Xandra.values() | nil,
          default_consistency: atom() | nil,
          protocol_module: module() | nil
        }

  defimpl DBConnection.Query do
    alias Xandra.Frame

    def parse(query, _options) do
      query
    end

    def encode(query, values, options) do
      Frame.new(:query, Keyword.take(options, [:compressor, :tracing]))
      |> query.protocol_module.encode_request(%{query | values: values}, options)
      |> Frame.encode(query.protocol_module)
    end

    def decode(query, %Frame{} = frame, options) do
      query.protocol_module.decode_response(frame, query, options)
    end

    def describe(query, _options) do
      query
    end
  end
end
