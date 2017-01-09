defmodule Xandra.Prepared.Cache do
  alias Xandra.Prepared

  def new() do
    :ets.new(__MODULE__, [:set, :public, read_concurrency: true])
  end

  def insert(table, %Prepared{} = prepared) do
    %{statement: statement,
      id: id,
      bound_columns: bound_columns,
      result_columns: result_columns} = prepared
    :ets.insert(table, {statement, id, bound_columns, result_columns})
    :ok
  end

  def lookup(table, %Prepared{statement: statement} = prepared) do
    case :ets.lookup(table, statement) do
      [{^statement, id, bound_columns, result_columns}] ->
        {:ok, %{prepared | id: id, bound_columns: bound_columns, result_columns: result_columns}}
      [] ->
        :error
    end
  end
end
