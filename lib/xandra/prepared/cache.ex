defmodule Xandra.Prepared.Cache do
  @moduledoc false

  alias Xandra.Prepared

  @type t :: :ets.tid()

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
      result_columns: result_columns
    } = prepared

    :ets.insert(table, {statement, id, bound_columns, result_columns})
    :ok
  end

  @spec lookup(t, Prepared.t()) :: {:ok, Prepared.t()} | :error
  def lookup(table, %Prepared{statement: statement} = prepared) do
    case :ets.lookup(table, statement) do
      [{^statement, id, bound_columns, result_columns}] ->
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
