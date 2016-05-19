defmodule Xandra.Query do
  defstruct [:statement]

  def new(statement) do
    {:ok, %__MODULE__{statement: statement}}
  end

  defimpl DBConnection.Query do
    def encode(_query, _params, _opts) do
    end

    def decode(_query, _result, _opts) do
    end
  end
end
