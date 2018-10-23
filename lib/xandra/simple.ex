defmodule Xandra.Simple do
  @moduledoc false

  defstruct [:statement, :values]

  @opaque t :: %__MODULE__{
            statement: Xandra.statement(),
            values: Xandra.values() | nil
          }

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(query, _options) do
      raise "cannot prepare #{inspect(query)}"
    end

    def encode(query, values, options) do
      Frame.new(:query)
      |> Protocol.encode_request(%{query | values: values}, options)
      |> Frame.encode(options[:compressor])
    end

    def decode(query, %Frame{} = frame, options) do
      Protocol.decode_response(frame, query, options)
    end

    def describe(query, _options) do
      raise "cannot prepare #{inspect(query)}"
    end
  end
end
