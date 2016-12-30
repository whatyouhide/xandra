defmodule Xandra.TypeParser do
  @moduledoc false

  @builtin_types [
    :text,
    :blob,
    :ascii,
    :bigint,
    :counter,
    :int,
    :varint,
    :boolean,
    :decimal,
    :double,
    :float,
    :inet,
    :timestamp,
    :uuid,
    :timeuuid,
    :date,
    :smallint,
    :time,
    :tinyint,
    :map,
    :set,
    :list,
    :tuple,
    :empty,
    :frozen,
    :varchar,
  ]

  def parse(string) when is_binary(string) do
    {type, rest} =
      string
      |> String.replace(" ", "")
      |> String.downcase()
      |> parse_type()

    if rest != "" do
      raise "invalid type"
    end

    type
  end

  defp parse_type(chars) do
    case parse_plain_type(chars) do
      {type, "<" <> rest} ->
        case parse_inner(rest) do
          {types, ">" <> rest} ->
            {{type, types}, rest}
          _ ->
            raise "invalid type"
        end
      {type, rest} ->
        {type, rest}
    end
  end

  for type <- @builtin_types do
    defp parse_plain_type(unquote(Atom.to_string(type)) <> rest), do: {unquote(type), rest}
  end

  defp parse_plain_type(_other) do
    raise "invalid type"
  end

  defp parse_inner(chars) do
    case parse_type(chars) do
      {type, "," <> rest} ->
        {types, rest} = parse_inner(rest)
        {[type | types], rest}
      {type, rest} ->
        {[type], rest}
    end
  end
end
