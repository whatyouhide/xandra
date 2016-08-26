defmodule Xandra.Query do
  defstruct [:statement]

  def new(statement) do
    {:ok, %__MODULE__{statement: statement}}
  end

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def encode(query, _params, _opts) do
      body = Protocol.encode(query.statement)
      %Frame{opcode: 0x07} |> Frame.encode(body)
    end

    def decode(_query, _result, _opts) do
    end
  end
end
