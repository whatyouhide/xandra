defmodule Xandra.Prepared do
  @moduledoc """
  A data structure used to internally represent prepared queries.
  """

  defstruct [:statement, :values, :id, :bound_columns, :result_columns]

  @opaque t :: %__MODULE__{
    statement: Xandra.statement,
    values: Xandra.values | nil,
    id: binary | nil,
    bound_columns: list | nil,
    result_columns: list | nil,
  }

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(prepared, _options) do
      prepared
    end

    def encode(prepared, values, options) do
      Frame.new(:execute)
      |> Protocol.encode_request(%{prepared | values: values}, options)
      |> Frame.encode()
    end

    def decode(prepared, %Frame{} = frame, _options) do
      Protocol.decode_response(frame, prepared)
    end

    def describe(prepared, _options) do
      prepared
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(prepared, options) do
      concat(["#Xandra.Prepared<", to_doc(prepared.statement, options), ">"])
    end
  end
end
