defmodule Xandra.Stream do
  @moduledoc false

  defstruct [:conn, :query, :params, :options, state: :new]

  defimpl Enumerable do
    alias Xandra.Page

    def reduce(stream, acc, fun) do
      Stream.resource(fn() -> start(stream) end, &next/1, &close/1).(acc, fun)
    end

    def member?(_stream, _term) do
      {:error, __MODULE__}
    end

    def count(_stream) do
      {:error, __MODULE__}
    end

    defp start(%{state: :new} = stream) do
      %{stream | state: :run}
    end

    defp next(%{state: :done} = stream) do
      {:halt, stream}
    end

    defp next(stream) do
      %{conn: conn, query: query, params: params, options: options} = stream

      case Xandra.execute!(conn, query, params, options) do
        %Page{paging_state: nil} = page ->
          {[page], %{stream | state: :done}}
        %Page{paging_state: paging_state} = page ->
          options = Keyword.put(options, :paging_state, paging_state)
          {[page], %{stream | options: options}}
      end
    end

    defp close(stream) do
      stream
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(stream, options) do
      properties = [
        query: stream.query,
        params: stream.params,
        options: stream.options,
      ]
      concat(["#Xandra.Stream<", to_doc(properties, options), ">"])
    end
  end
end
