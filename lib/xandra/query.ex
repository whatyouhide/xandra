defmodule Xandra.Query do
  defstruct [:statement, :values, :result_columns]

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(query, _opts) do
      query
    end

    def encode(query, values, opts) do
      Frame.new(:query)
      |> Protocol.encode_request(%{query | values: values}, opts)
      |> Frame.encode()
    end

    def decode(query, %Frame{} = frame, _opts) do
      Protocol.decode_response(frame, query)
    end

    def describe(query, _opts) do
      query
    end
  end
end
