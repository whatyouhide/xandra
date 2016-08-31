defmodule Xandra.Query do
  defstruct [:name, :statement, :result]

  def new(name \\ nil, statement) do
    {:ok, %__MODULE__{name: name, statement: statement}}
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

    def decode(_query, _result, _opts) do
    end

    def describe(_query, _opts) do
    end
  end
end
