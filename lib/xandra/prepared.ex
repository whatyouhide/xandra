defmodule Xandra.Prepared do
  defstruct [:statement, :values, :id, :bound_columns, :result_columns]

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(prepared, _opts) do
      prepared
    end

    def encode(prepared, values, opts) do
      Frame.new(:execute)
      |> Protocol.encode_request(%{prepared | values: values}, opts)
      |> Frame.encode()
    end

    def decode(prepared, %Frame{} = frame, _opts) do
      Protocol.decode_response(frame, prepared)
    end

    def describe(prepared, _opts) do
      prepared
    end
  end
end
