defmodule Xandra.Page do
  @moduledoc ~S"""
  A struct that represents a page of rows.

  This struct represents a page of rows that have been returned by the
  Cassandra server in response to a query such as `SELECT`, but have not yet
  been parsed into Elixir values.

  This struct implements the `Enumerable` protocol and is therefore a stream. It
  is through this protocol that a `Xandra.Page` struct can be parsed into Elixir
  values. The simplest way of getting a list of single rows out of a
  `Xandra.Page` struct is to use something like `Enum.to_list/1`. Each element
  emitted when streaming out of a `Xandra.Page` struct is a map of string column
  names to their corresponding value.

  ## Examples

      statement = "SELECT name, age FROM users"
      %Xandra.Page{} = page = Xandra.execute!(conn, statement, _params = [])
      Enum.each(page, fn %{"name" => name, "age" => age} ->
        IO.puts "Read user with name #{name} (age #{age}) out of the database"
      end)

  """

  defstruct [:content, :columns, :paging_state]

  @opaque t :: %__MODULE__{}

  @doc """
  Tells whether more pages are available to fetch after the given one.

  This function can be used to check whether more pages are available to fetch
  after the given page. This is useful when implementing manual paging. See also
  the documentation for `Xandra.execute/4`.
  """
  @spec more_pages_available?(t) :: boolean
  def more_pages_available?(%__MODULE__{paging_state: paging_state}) do
    paging_state != nil
  end

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

    def member?(_page, _term) do
      {:error, __MODULE__}
    end

    def count(_page) do
      {:error, __MODULE__}
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(page, options) do
      properties = [
        rows: Enum.to_list(page),
        more_pages_available: @for.more_pages_available?(page),
      ]
      concat(["#Xandra.Page<", to_doc(properties, options), ">"])
    end
  end
end
