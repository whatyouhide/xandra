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
  def parse(string) do
    case :xandra_type_parser.parse(tokenize(string)) do
      {:ok, parsed} ->
        validate_types(parsed)
      {:error, {line, _module, reason}} ->
        raise ArgumentError, IO.chardata_to_string(reason)
    end
  end

  defp tokenize(string) do
    string
    |> String.downcase()
    |> tokenize(_tokens = [])
    |> Enum.reverse()
  end

  defp tokenize(<<" ", rest::binary>>, tokens) do
    tokenize(rest, tokens)
  end

  defp tokenize(<<char, rest::binary>>, tokens) when char in [?<, ?>, ?,] do
    tokenize(rest, [{String.to_atom(<<char>>), 1} | tokens])
  end

  defp tokenize(<<_char, _::binary>> = string, tokens) do
    {word, rest} = next_word(string, _acc = "")
    tokenize(rest, [{:type, 1, word} | tokens])
  end

  defp tokenize(<<>>, tokens) do
    tokens
  end

  defp next_word(<<char, rest::binary>>, acc)
      when char in ?a..?z or char in ?A..?Z or char in ?0..?9,
    do: next_word(rest, <<acc::binary, char>>)
  defp next_word(rest, acc),
    do: {acc, rest}

  defp validate_types(collection) when is_list(collection) do
    collection
    |> Enum.map(&validate_types/1)
    |> List.to_tuple()
  end

  defp validate_types(type) when type in @builtin_types do
    String.to_atom(type)
  end

  defp validate_types(type) do
    raise ArgumentError, "unknown type: #{inspect(type)}"
  end
end
