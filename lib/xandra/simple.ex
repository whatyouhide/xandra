defmodule Xandra.Simple do
  @moduledoc false

  defstruct [:statement, :values]

  @opaque t :: %__MODULE__{
    statement: Xandra.statement,
    values: Xandra.values | nil,
  }

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(query, _options) do
      query
    end

    def encode(query, values, options) do
      Frame.new(:query)
      |> Protocol.encode_request(%{query | values: values}, options)
      |> Frame.encode()
    end

    def decode(query, %Frame{} = frame, _options) do
      Protocol.decode_response(frame, query)
    end

    def describe(query, _options) do
      query
    end
  end
end
