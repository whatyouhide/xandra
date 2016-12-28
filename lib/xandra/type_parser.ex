defmodule Xandra.TypeParser do
  @moduledoc false

  # For now, this type parser doesn't support user-defined types.

  @basic_types ~w(
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
      |> parse_with_acc(_acc = [])
    type
  end

  for type <- @basic_types do
    defp parse_with_acc(unquote(Atom.to_string(type)) <> rest, acc) do
      parse_with_acc(rest, [unquote(type) | acc])
    end
  end

  # Start of composite type
  defp parse_with_acc("<" <> rest, acc) do
    {children, rest} = parse_with_acc(rest, [])
    [type | types] = acc
    parse_with_acc(rest, [List.to_tuple([type | children]) | types])
  end

  # One type finished, we just go on.
  defp parse_with_acc(", " <> rest, acc) do
    parse_with_acc(rest, acc)
  end

  # Contents of composite type are finished, we return the children and the
  # rest.
  defp parse_with_acc(">" <> rest, acc) do
    {:lists.reverse(acc), rest}
  end

  defp parse_with_acc("", acc) do
    {:lists.reverse(acc), ""}
  end

  defp parse_with_acc(other, _acc) do
    raise ArgumentError, "unparsable type: #{inspect(other)}"
  end
end
