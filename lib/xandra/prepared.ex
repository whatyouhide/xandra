defmodule Xandra.Prepared do
  defstruct [:statement, :values, :id, :bound_columns, :result_columns]

  @opaque t :: %__MODULE__{
    statement: String.t,
    values: Xandra.values,
    id: binary,
    bound_columns: list,
    result_columns: list,
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
end
