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
  )a

  @spec parse(String.t) :: atom | tuple
  def parse(string) do
    {[type], ""} =
      string
      |> String.downcase()
      |> parse_next(_acc = [])
    type
  end

  for type <- @builtin_types do
    defp parse_next(unquote(Atom.to_string(type)) <> rest, acc) do
      parse_next(rest, [unquote(type) | acc])
    end
  end

  # Start of composite type.
  defp parse_next("<" <> rest, [type | types] = _acc) do
    {children, rest} = parse_next(rest, [])
    parse_next(rest, [List.to_tuple([type | children]) | types])
  end

  # One type finished, we just go on.
  defp parse_next(", " <> rest, acc) do
    parse_next(rest, acc)
  end

  # Contents of composite type are finished, we return the children and the
  # rest.
  defp parse_next(">" <> rest, acc) do
    {:lists.reverse(acc), rest}
  end

  defp parse_next("", acc) do
    {:lists.reverse(acc), ""}
  end

  defp parse_next(other, _acc) do
    raise ArgumentError, "unparsable type: #{inspect(other)}"
  end
end
