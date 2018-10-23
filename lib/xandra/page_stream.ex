defmodule Xandra.PageStream do
  @moduledoc false

  defstruct [:conn, :query, :params, :options, state: :new]

  defimpl Enumerable do
    alias Xandra.Page

    def reduce(page_stream, acc, fun) do
      Stream.resource(fn -> start(page_stream) end, &next/1, &close/1).(acc, fun)
    end

    def member?(_page_stream, _value), do: {:error, __MODULE__}

    def count(_page_stream), do: {:error, __MODULE__}

    def slice(_page_stream), do: {:error, __MODULE__}

    defp start(%{state: :new} = page_stream) do
      %{page_stream | state: :run}
    end

    defp next(%{state: :done} = page_stream) do
      {:halt, page_stream}
    end

    defp next(page_stream) do
      %{conn: conn, query: query, params: params, options: options} = page_stream

      case Xandra.execute!(conn, query, params, options) do
        %Page{paging_state: nil} = page ->
          {[page], %{page_stream | state: :done}}

        %Page{paging_state: paging_state} = page ->
          options = Keyword.put(options, :paging_state, paging_state)
          {[page], %{page_stream | options: options}}
      end
    end

    defp close(page_stream) do
      page_stream
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(page_stream, options) do
      properties = [
        query: page_stream.query,
        params: page_stream.params,
        options: page_stream.options
      ]

      concat(["#Xandra.PageStream<", to_doc(properties, options), ">"])
    end
  end
end
