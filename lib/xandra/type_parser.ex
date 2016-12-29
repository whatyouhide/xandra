defmodule Xandra.TypeParser do
  @moduledoc false

  # TODO: support user-defined types.

  @builtin_types ~w(
    text
    blob
    ascii
    bigint
    counter
    int
    varint
    boolean
    decimal
    double
    float
    inet
    timestamp
    uuid
    timeuuid
    date
    smallint
    time
    tinyint
    map
    set
    list
    tuple
    empty
    frozen
    varchar
  )

  @spec parse(String.t) :: atom | tuple
  def parse(string) when is_binary(string) do
    {types, _rest = []} =
      string
      |> String.downcase()
      |> tokenize(_acc = [])
      |> parse_tokens(_acc = [], _nesting = 0)

    case types do
      [type] -> type
      [] -> raise ArgumentError, "invalid type: no types"
      _types -> raise ArgumentError, "invalid type: more than one type"
    end
  end

  defp tokenize("", tokens),
    do: Enum.reverse(tokens)

  defp tokenize(" " <> rest, tokens),
    do: tokenize(rest, tokens)

  defp tokenize(<<keyword, rest::binary>>, tokens) when keyword in '<>,',
    do: tokenize(rest, [String.to_atom(<<keyword>>) | tokens])

  for type <- @builtin_types do
    defp tokenize(unquote(type) <> rest, tokens),
      do: tokenize(rest, [{:builtin, unquote(String.to_atom(type))} | tokens])
  end

  defp parse_tokens([{:builtin, builtin}, :< | rest], acc, nesting) do
    {children, rest} = parse_tokens(rest, _acc = [], nesting + 1)
    collection = List.to_tuple([builtin | children])
    parse_tokens(rest, [collection | acc], nesting)
  end

  defp parse_tokens([:< | _rest], _acc, _nesting) do
    raise "missing type before <"
  end

  defp parse_tokens([:"," | [{:builtin, _builtin} | _] = _rest], _acc = [], _nesting) do
    raise ArgumentError, "invalid type: unexpected ,"
  end

  defp parse_tokens([:"," | [{:builtin, _builtin} | _] = rest], acc, nesting) do
    parse_tokens(rest, acc, nesting)
  end

  defp parse_tokens([:"," | _rest], _acc, _nesting) do
    raise ArgumentError, "invalid type: unexpected ,"
  end

  defp parse_tokens([{:builtin, builtin} | rest], acc, nesting) do
    parse_tokens(rest, [builtin | acc], nesting)
  end

  defp parse_tokens([:> | _rest], _acc = [], _nesting) do
    raise ArgumentError, "invalid type: unexpected >"
  end

  defp parse_tokens([:> | _rest], _acc, _nesting = 0) do
    raise ArgumentError, "invalid type: unexpected >"
  end

  defp parse_tokens([:> | rest], acc, _nesting) do
    {Enum.reverse(acc), rest}
  end

  defp parse_tokens([], acc, _nesting) do
    {Enum.reverse(acc), []}
  end
end
