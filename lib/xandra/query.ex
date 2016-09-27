defmodule Xandra.Query do
  defstruct [:statement, :prepared]

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(query, _opts) do
      query
    end

    def encode(query, values, opts) do
      body = Protocol.encode_query(query, values, opts)
      operation = if query.prepared, do: :execute, else: :query
      Frame.new(operation, body)
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
