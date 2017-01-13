defmodule Xandra.Rows do
  defstruct [:content, :columns, :paging_state]

  @opaque t :: %__MODULE__{}

  defimpl Enumerable do
    def reduce(%{content: content, columns: columns}, acc, fun) do
      reduce(content, columns, acc, fun)
    end

    defp reduce(_content, _columns, {:halt, acc}, _fun) do
      {:halted, acc}
    end

    defp reduce(content, columns, {:suspend, acc}, fun) do
      {:suspended, acc, &reduce(content, columns, &1, fun)}
    end

    defp reduce([values | rest], columns, {:cont, acc}, fun) do
      row = zip(values, columns, []) |> :maps.from_list
      reduce(rest, columns, fun.(row, acc), fun)
    end

    defp reduce([], _columns, {:cont, acc}, _fun) do
      {:done, acc}
    end

    defp zip([], [], acc), do: acc

    defp zip([value | values], [{_, _, name, _} | columns], acc) do
      zip(values, columns, [{name, value} | acc])
    end

    def member?(_rows, _term) do
      {:error, __MODULE__}
    end

    def count(_rows) do
      {:error, __MODULE__}
    end
  end
end
