defmodule Xandra.Stream do
  defstruct [:conn, :query, :params, :opts, state: :new]

  defimpl Enumerable do
    alias Xandra.Rows

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
      %{conn: conn, query: query, params: params, opts: opts} = stream
      case Xandra.execute(conn, query, params, opts) |> elem(1) do
        %Rows{paging_state: nil} = rows ->
          {[rows], %{stream | state: :done}}
        %Rows{paging_state: paging_state} = rows ->
          opts = Keyword.put(opts, :paging_state, paging_state)
          {[rows], %{stream | opts: opts}}
      end
    end

    defp close(stream) do
      stream
    end
  end
end
