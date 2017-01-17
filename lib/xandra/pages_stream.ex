defmodule Xandra.PagesStream do
  @moduledoc false

  defstruct [:conn, :query, :params, :options, state: :new]

  defimpl Enumerable do
    alias Xandra.Page

    def reduce(pages_stream, acc, fun) do
      Stream.resource(fn() -> start(pages_stream) end, &next/1, &close/1).(acc, fun)
    end

    def member?(_pages_stream, _term) do
      {:error, __MODULE__}
    end

    def count(_pages_stream) do
      {:error, __MODULE__}
    end

    defp start(%{state: :new} = pages_stream) do
      %{pages_stream | state: :run}
    end

    defp next(%{state: :done} = pages_stream) do
      {:halt, pages_stream}
    end

    defp next(pages_stream) do
      %{conn: conn, query: query, params: params, options: options} = pages_stream

      case Xandra.execute!(conn, query, params, options) do
        %Page{paging_state: nil} = page ->
          {[page], %{pages_stream | state: :done}}
        %Page{paging_state: paging_state} = page ->
          options = Keyword.put(options, :paging_state, paging_state)
          {[page], %{pages_stream | options: options}}
      end
    end

    defp close(pages_stream) do
      pages_stream
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(pages_stream, options) do
      properties = [
        query: pages_stream.query,
        params: pages_stream.params,
        options: pages_stream.options,
      ]
      concat(["#Xandra.PagesStream<", to_doc(properties, options), ">"])
    end
  end
end
