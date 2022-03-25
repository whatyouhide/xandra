defmodule Xandra.Prepared.Cache do
  @moduledoc false

  alias Xandra.Prepared

  @opaque t :: :ets.tid()

  @spec new() :: t
  def new() do
    :ets.new(__MODULE__, [:set, :public, read_concurrency: true])
  end

  @spec insert(t, Prepared.t()) :: :ok
  def insert(table, %Prepared{} = prepared) do
    %{
      statement: statement,
      id: id,
      bound_columns: bound_columns,
      result_columns: result_columns,
      keyspace: keyspace
    } = prepared

    key = {statement, keyspace}

    :ets.insert(table, {key, id, bound_columns, result_columns})
    :ok
  end

  @spec lookup(t, Prepared.t()) :: {:ok, Prepared.t()} | :error
  def lookup(table, %Prepared{statement: statement, keyspace: keyspace} = prepared) do
    key = {statement, keyspace}

    case :ets.lookup(table, key) do
      [{^key, id, bound_columns, result_columns}] ->
        {:ok, %{prepared | id: id, bound_columns: bound_columns, result_columns: result_columns}}

      [] ->
        :error
    end
  end

  @spec delete(t, Prepared.t()) :: :ok
  def delete(table, %Prepared{statement: statement}) do
    :ets.delete(table, statement)
    :ok
  end
end
