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
    try do
      string
      |> String.downcase()
      |> parse_node()
      |> tree_to_simple_representation()
    catch
      :throw, {:error, message} when is_binary(message) ->
        raise ArgumentError, "invalid type #{inspect(string)}: #{message}"
    end
  end

  defp tree_to_simple_representation(%{name: name, children: children}) when name in @builtin_types do
    type = String.to_atom(name)

    if children == [] do
      type
    else
      List.to_tuple([type | Enum.map(children, &tree_to_simple_representation/1)])
    end
  end

  defp tree_to_simple_representation(%{name: name} = _node) do
    throw({:error, "unknown type #{inspect(name)}"})
  end

  @node %{
    name: "",
    children: [],
    parent: nil,
  }

  def parse_node(string) do
    parse_node(string, _current = @node)
  end

  # Starting the subtypes of the current type.
  defp parse_node("<" <> rest, current_node) do
    if current_node.name == "" do
      throw({:error, "syntax error for children of empty type"})
    else
      first_child = %{@node | parent: current_node}
      parse_node(rest, first_child)
    end
  end

  # One subtype of "parent" finished, on to the next.
  defp parse_node("," <> rest, current_node) do
    assert_parent_and_not_empty_subtype(current_node)
    parent = current_node.parent
    parent = append_child_to_node(parent, current_node)
    next_child = %{@node | parent: parent}
    parse_node(rest, next_child)
  end

  # Subtypes finished.
  defp parse_node(">" <> rest, current_node) do
    assert_parent_and_not_empty_subtype(current_node)
    parent = current_node.parent
    parent = append_child_to_node(parent, current_node)
    parse_node(rest, parent)
  end

  # We skip spaces.
  defp parse_node(" " <> rest, current_node) do
    parse_node(rest, current_node)
  end

  defp parse_node(<<char, rest::binary>>, current_node) do
    current_node = %{current_node | name: <<current_node.name::binary, char>>}
    parse_node(rest, current_node)
  end

  defp parse_node("", current_node) do
    case current_node do
      %{name: ""} ->
        throw({:error, "unexpected end of input"})
      %{parent: parent} when not is_nil(parent) ->
        throw({:error, "unexpected end of input"})
      _other ->
        current_node
    end
  end

  defp append_child_to_node(%{children: children} = node, new_child) do
    new_child = %{new_child | parent: nil}
    %{node | children: children ++ [new_child]}
  end

  defp assert_parent_and_not_empty_subtype(%{parent: nil}) do
    throw({:error, "syntax error for misplaced >"})
  end

  defp assert_parent_and_not_empty_subtype(%{name: ""}) do
    throw({:error, "syntax error for empty subtype"})
  end

  defp assert_parent_and_not_empty_subtype(_node) do
    :ok
  end
end
