defmodule Xandra.Batch do
  alias Xandra.{Prepared, Simple}

  @enforce_keys [:type]
  defstruct @enforce_keys ++ [queries: []]

  @type type :: :logged | :unlogged | :counter

  @opaque t :: %__MODULE__{
    type: type,
    queries: [Simple.t | Prepared.t],
  }

  def new(type \\ :logged) when type in [:logged, :unlogged, :counter] do
    %__MODULE__{type: type}
  end

  def add(batch, statement, values \\ [])

  def add(%__MODULE__{} = batch, statement, values)
      when is_binary(statement) and is_list(values) do
    add_query(batch, %Simple{statement: statement}, values)
  end

  def add(%__MODULE__{} = batch, %Prepared{} = prepared, values)
      when is_list(values) do
    add_query(batch, prepared, values)
  end

  defp add_query(batch, query, values) do
    queries = [%{query | values: values} | batch.queries]
    %{batch | queries: queries}
  end

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(batch, _options) do
      batch
    end

    def encode(batch, _values, options) do
      batch = %{batch | queries: Enum.reverse(batch.queries)}

      Frame.new(:batch)
      |> Protocol.encode_request(batch, options)
      |> Frame.encode()
    end

    def decode(batch, %Frame{} = frame, _options) do
      Protocol.decode_response(frame, batch)
    end

    def describe(batch, _options) do
      batch
    end
  end
end
