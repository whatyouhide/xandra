defmodule Xandra.Query do
  defstruct [:id, :statement, :metadata]

  def new(statement) when is_binary(statement) do
    {:ok, %__MODULE__{statement: statement}}
  end

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(query, _opts) do
      query
    end

    def encode(query, params, _opts) do
      body = Protocol.encode(query.statement, params)
      %Frame{opcode: 0x07} |> Frame.encode(body)
    end

    def decode(_query, {header, body}, _opts) do
      Protocol.decode_response(header, body)
    end

    def describe(query, _opts) do
      query
    end
  end
end
