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

  See [`%Xandra.Page{}`](`__struct__/0`) for information about which fields are public.

  ## Examples

      statement = "SELECT name, age FROM users"

      %Xandra.Page{} = page = Xandra.execute!(conn, statement, _params = [])

      Enum.each(page, fn %{"name" => name, "age" => age} ->
        IO.puts("Read user with name #{name} (age #{age}) out of the database")
      end)

  """

  @doc """
  The page struct.

  The following fields are public and can be accessed or relied on:

    * `:paging_state` - the current paging state, or `nil` if no paging is occurring.
      Its value can be used to check whether more pages are available to fetch
      after the given page. This is useful when implementing manual paging.
      See also the documentation for `Xandra.execute/4`.

    * `:tracing_id` - the tracing ID (as a UUID binary) if tracing was enabled,
      or `nil` if no tracing was enabled. See the "Tracing" section in `Xandra.execute/4`.

  """
  defstruct [:paging_state, :tracing_id, :custom_payload, content: [], columns: []]

  @typedoc """
  The paging state of a page.

  This is intended to be an "opaque" binary value that you can use for further
  pagination. See `Xandra.execute/4`.
  """
  @type paging_state() :: binary()

  @typedoc """
  The type for the page struct.

  The only public fields here are `:paging_state` and `:tracing_id`.
  See [`%Xandra.Page{}`](`__struct__/0`).
  """
  @type t :: %__MODULE__{
          content: list(),
          columns: list(),
          paging_state: paging_state() | nil,
          tracing_id: binary() | nil,
          custom_payload: Xandra.custom_payload() | nil
        }

  defimpl Enumerable do
    def reduce(%{content: content, columns: columns}, acc, fun) do
      reduce(content, columns, acc, fun)
    end

    def member?(_page, _value), do: {:error, __MODULE__}

    def count(_page), do: {:error, __MODULE__}

    def slice(_page), do: {:error, __MODULE__}

    defp reduce(_content, _columns, {:halt, acc}, _fun) do
      {:halted, acc}
    end

    defp reduce(content, columns, {:suspend, acc}, fun) do
      {:suspended, acc, &reduce(content, columns, &1, fun)}
    end

    defp reduce([values | rest], columns, {:cont, acc}, fun) do
      row = zip(values, columns, []) |> :maps.from_list()
      reduce(rest, columns, fun.(row, acc), fun)
    end

    defp reduce([], _columns, {:cont, acc}, _fun) do
      {:done, acc}
    end

    defp zip([], [], acc), do: acc

    defp zip([value | values], [{_, _, name, _} | columns], acc) do
      zip(values, columns, [{name, value} | acc])
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(page, options) do
      properties = [
        rows: Enum.to_list(page),
        tracing_id: page.tracing_id,
        more_pages?: page.paging_state != nil
      ]

      concat(["#Xandra.Page<", to_doc(properties, options), ">"])
    end
  end
end
