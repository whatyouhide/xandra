defmodule Xandra.Query do
  defstruct [:statement, :values]

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
