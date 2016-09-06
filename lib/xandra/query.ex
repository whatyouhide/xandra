defmodule Xandra.Query do
  defstruct [:statement, :prepared]

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(query, _opts) do
      query
    end

    def encode(%{prepared: nil} = query, values, opts) do
      body = Protocol.encode_query(query.statement, values, opts)
      %Frame{opcode: 0x07} |> Frame.encode(body)
    end

    def encode(query, values, opts) do
      {query_id, _result} = query.prepared
      body = Protocol.encode_prepared_query(query_id, values, opts)
      %Frame{opcode: 0x0A} |> Frame.encode(body)
    end

    def decode(query, {header, body}, _opts) do
      Protocol.decode_response(header, body, query)
    end

    def describe(query, _opts) do
      query
    end
  end
end
