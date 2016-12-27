defmodule Xandra.Protocol do
  use Bitwise

  alias Xandra.{Frame, Query, Rows, Error}

  def encode_request(frame, params, opts \\ [])

  def encode_request(%Frame{kind: :options} = frame, nil, _opts) do
    %{frame | body: ""}
  end

  def encode_request(%Frame{kind: :startup} = frame, params, _opts) when is_map(params) do
    %{"CQL_VERSION" => [cql_version | _]} = params
    %{frame | body: encode_string_map(%{"CQL_VERSION" => cql_version})}
  end

  def encode_request(%Frame{kind: :query} = frame, %Query{} = query, opts) do
    %{statement: statement, values: values} = query
    body =
      <<byte_size(statement)::32>> <>
      statement <>
      encode_params(values, opts, false)
    %{frame | body: body}
  end

  def encode_request(%Frame{kind: :prepare} = frame, %Query{} = query, _opts) do
    %{statement: statement} = query
    body = <<byte_size(statement)::32>> <> statement
    %{frame | body: body}
  end

  def encode_request(%Frame{kind: :execute} = frame, %Query{} = query, opts) do
    %{prepared: {id, _rows}, values: values} = query
    body =
      <<byte_size(id)::16>> <>
      id <>
      encode_params(values, opts, true)
    %{frame | body: body}
  end

  def encode_string_map(map) do
    for {key, value} <- map, into: <<map_size(map)::16>> do
      key_size = byte_size(key)
      <<key_size::16, key::size(key_size)-bytes, byte_size(value)::16, value::bytes>>
    end
  end

  @consistency_levels %{
    0x0000 => :any,
    0x0001 => :one,
    0x0002 => :two,
    0x0003 => :three,
    0x0004 => :quorum,
    0x0005 => :all,
    0x0006 => :local_quorum,
    0x0007 => :each_quorum,
    0x0008 => :serial,
    0x0009 => :local_serial,
    0x000A => :local_one,
  }

  for {spec, level} <- @consistency_levels do
    defp encode_consistency_level(unquote(level)) do
      <<unquote(spec)::16>>
    end
  end

  defp set_query_values(mask, values) do
    cond do
      values == [] ->
        mask
      is_list(values) ->
        mask ||| 0x01
      map_size(values) == 0 ->
        mask
      is_map(values) ->
        mask ||| 0x01 ||| 0x40
    end
  end

  defp set_metadata_presence(mask, skip_metadata?) do
    if skip_metadata? do
      mask ||| 0x02
    else
      mask
    end
  end

  defp set_paging_state(mask, value) do
    if value do
      mask ||| 0x08
    else
      mask
    end
  end

  defp encode_params(values, opts, skip_metadata?) do
    consistency = Keyword.get(opts, :consistency, :one)
    page_size = Keyword.get(opts, :page_size, 10_000)
    paging_state = Keyword.get(opts, :paging_state)

    flags =
      set_query_values(0x00, values)
      |> bor(0x04)
      |> set_metadata_presence(skip_metadata?)
      |> set_paging_state(paging_state)

    encode_consistency_level(consistency) <>
      <<flags>> <>
      encode_values(values) <>
      <<page_size::32>> <>
      encode_paging_state(paging_state)
  end

  defp encode_paging_state(value) do
    if value do
      <<byte_size(value)::32>> <> value
    else
      <<>>
    end
  end

  defp encode_values(values) when values == [] or map_size(values) == 0 do
    <<>>
  end

  defp encode_values(values) when is_list(values) do
    for value <- values, into: <<length(values)::16>> do
      value = encode_query_value(value)
      <<byte_size(value)::32>> <> value
    end
  end

  defp encode_values(values) when is_map(values) do
    for {name, value} <- values, into: <<map_size(values)::16>> do
      name = to_string(name)
      value = encode_query_value(value)
      <<byte_size(name)::16>> <> name <>
        <<byte_size(value)::32>> <> value
    end
  end

  defp encode_query_value({:ascii, value}) when is_binary(value) do
    value
  end

  defp encode_query_value({:bigint, value}) when is_integer(value) do
    <<value::64>>
  end

  defp encode_query_value({:blob, value}) when is_binary(value) do
    value
  end

  defp encode_query_value({:boolean, value}) when is_boolean(value) do
    if value, do: <<1>>, else: <<0>>
  end

  defp encode_query_value({:decimal, {value, scale}}) do
    encode_query_value({:int, scale}) <> encode_query_value({:varint, value})
  end

  defp encode_query_value({:double, value}) when is_float(value) do
    <<value::64-float>>
  end

  defp encode_query_value({:float, value}) when is_float(value) do
    <<value::32-float>>
  end

  defp encode_query_value({:inet, {n1, n2, n3, n4} = _value}) do
    <<n1, n2, n3, n4>>
  end

  defp encode_query_value({:inet, {n1, n2, n3, n4, n5, n6, n7, n8}}) do
    <<n1::4-bytes, n2::4-bytes, n3::4-bytes, n4::4-bytes,
      n5::4-bytes, n6::4-bytes, n7::4-bytes, n8::4-bytes>>
  end

  defp encode_query_value({:int, value}) do
    <<value::32>>
  end

  defp encode_query_value({:list, value}) do
    raise "pending type to encode: #{inspect(value)}"
  end

  defp encode_query_value({:map, value}) do
    raise "pending type to encode: #{inspect(value)}"
  end

  defp encode_query_value({:set, value}) do
    raise "pending type to encode: #{inspect(value)}"
  end

  defp encode_query_value({:text, value}) when is_binary(value) do
    value
  end

  defp encode_query_value({:timestamp, value}) do
    encode_query_value({:bigint, value})
  end

  defp encode_query_value({:uuid, value}) when is_binary(value) do
    <<part1::8-bytes, ?-,
      part2::4-bytes, ?-,
      part3::4-bytes, ?-,
      part4::4-bytes, ?-,
      part5::12-bytes>> = value
    <<decode_base16(part1)::4-bytes,
      decode_base16(part2)::2-bytes,
      decode_base16(part3)::2-bytes,
      decode_base16(part4)::2-bytes,
      decode_base16(part5)::6-bytes>>
  end

  # Alias of :text
  defp encode_query_value({:varchar, value}) do
    encode_query_value({:text, value})
  end

  defp encode_query_value({:varint, value}) when is_integer(value) do
    size = varint_byte_size(value)
    <<value::size(size)-unit(8)>>
  end

  defp encode_query_value({:timeuuid, value}) when is_binary(value) do
    encode_query_value({:uuid, value})
  end

  defp encode_query_value({:tuple, value}) do
    raise "pending type to encode: #{inspect(value)}"
  end

  defp varint_byte_size(value) when value in -128..127,
    do: 1
  defp varint_byte_size(value) when value > 127,
    do: 1 + varint_byte_size(value >>> 8)
  defp varint_byte_size(value) when value < -128,
    do: varint_byte_size(-value - 1)

  @compile {:inline, decode_base16: 1}
  defp decode_base16(value) do
    Base.decode16!(value, case: :mixed)
  end

  def decode_response(frame, query \\ nil)

  def decode_response(%Frame{kind: :error, body: body} , _query) do
    <<code::32-signed>> <> buffer = body
    {message, ""} = decode_string(buffer)
    Error.new(code, message)
  end

  def decode_response(%Frame{kind: :ready, body: <<>>}, nil) do
    :ok
  end

  def decode_response(%Frame{kind: :supported, body: body}, nil) do
    {content, ""} = decode_string_multimap(body)
    content
  end

  def decode_response(%Frame{kind: :result, body: body}, %Query{} = query) do
    decode_result_response(body, query)
  end

  defp decode_result_response(<<0x0001::32-signed>>, _query) do
    %Xandra.Void{}
  end

  # Rows
  defp decode_result_response(<<0x0002::32-signed>> <> buffer, query) do
    rows = case query.prepared do
      {_query_id, rows} -> rows
      nil -> %Rows{}
    end
    {rows, buffer} = decode_metadata(rows, buffer)
    content = decode_rows_content(buffer, rows.column_specs)
    %{rows | content: content}
  end

  defp decode_result_response(<<0x0003::32-signed>> <> buffer, _query) do
    {keyspace, ""} = decode_string(buffer)
    %Xandra.SetKeyspace{keyspace: keyspace}
  end

  # Prepared
  defp decode_result_response(<<0x0004::32-signed>> <> buffer, query) do
    {query_id, buffer} = decode_string(buffer)
    {_rows, buffer} = decode_metadata(%Rows{}, buffer)
    {rows, <<>>} = decode_metadata(%Rows{}, buffer)
    %{query | prepared: {query_id, rows}}
  end

  defp decode_result_response(<<0x0005::32-signed>> <> buffer, _query) do
    {effect, buffer} = decode_string(buffer)
    {target, buffer} = decode_string(buffer)
    options = decode_change_options(buffer, target)
    %Xandra.SchemaChange{effect: effect, target: target, options: options}
  end

  defp decode_change_options(buffer, "KEYSPACE") do
    {keyspace, ""} = decode_string(buffer)
    %{keyspace: keyspace}
  end

  defp decode_change_options(buffer, target) when target in ["TABLE", "TYPE"] do
    {keyspace, buffer} = decode_string(buffer)
    {subject, ""} = decode_string(buffer)
    %{keyspace: keyspace, subject: subject}
  end

  defp decode_metadata(rows, <<flags::4-bytes, column_count::32-signed>> <> buffer) do
    <<_::29, no_metadata::1, has_more_pages::1, global_table_spec::1>> = flags
    {rows, buffer} = decode_paging_state(rows, has_more_pages, buffer)

    cond do
      no_metadata == 1 ->
        {rows, buffer}
      global_table_spec == 1 ->
        {keyspace, buffer} = decode_string(buffer)
        {table, buffer} = decode_string(buffer)
        {column_specs, buffer} = decode_column_specs(buffer, column_count, {keyspace, table}, [])
        {%{rows | column_specs: column_specs}, buffer}
      true ->
        {column_specs, buffer} = decode_column_specs(buffer, column_count, nil, [])
        {%{rows | column_specs: column_specs}, buffer}
    end
  end

  defp decode_paging_state(rows, 0, buffer) do
    {rows, buffer}
  end

  defp decode_paging_state(rows, 1, buffer) do
    <<byte_count::32, paging_state::bytes-size(byte_count)>> <> buffer = buffer
    {%{rows | paging_state: paging_state}, buffer}
  end

  defp decode_rows_content(<<row_count::32-signed>> <> buffer, column_specs) do
    {content, ""} = decode_rows_content(row_count, buffer, column_specs, column_specs, [[]])
    content
  end

  def decode_rows_content(0, buffer, column_specs, column_specs, [_ | acc]) do
    {Enum.reverse(acc), buffer}
  end

  def decode_rows_content(row_count, buffer, column_specs, [], [values | acc]) do
    decode_rows_content(row_count - 1, buffer, column_specs, column_specs, [[], Enum.reverse(values) | acc])
  end

  def decode_rows_content(row_count, <<size::32-signed>> <> buffer, column_specs, [{_, _, _, type} | rest], [values | acc]) do
    {value, buffer} = decode_value(size, buffer, type)
    values = [value | values]
    decode_rows_content(row_count, buffer, column_specs, rest, [values | acc])
  end

  defp decode_value(<<size::32-signed>> <> buffer, type) do
    decode_value(size, buffer, type)
  end

  defp decode_value(-1, buffer, _type) do
    {nil, buffer}
  end

  defp decode_value(size, buffer, :ascii) do
    <<value::size(size)-bytes>> <> buffer = buffer
    {value, buffer}
  end

  defp decode_value(8, <<value::64-signed>> <> buffer, :bigint) do
    {value, buffer}
  end

  defp decode_value(size, buffer, :blob) do
    <<value::size(size)-bytes>> <> buffer = buffer
    {value, buffer}
  end

  defp decode_value(1, <<value::8>> <> buffer, :boolean) do
    {value != 0, buffer}
  end

  defp decode_value(size, buffer, :decimal) do
    {scale, buffer} = decode_value(4, buffer, :int)
    {value, buffer} = decode_value(size - 4, buffer, :varint)
    {{value, scale}, buffer}
  end

  defp decode_value(8, <<value::64-float>> <> buffer, :double) do
    {value, buffer}
  end

  defp decode_value(4, <<value::32-float>> <> buffer, :float) do
    {value, buffer}
  end

  defp decode_value(4, <<address::4-bytes>> <> buffer, :inet) do
    <<n1, n2, n3, n4>> = address
    {{n1, n2, n3, n4}, buffer}
  end

  defp decode_value(16, <<address::16-bytes>> <> buffer, :inet) do
    <<n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16>> = address
    {{n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16}, buffer}
  end

  defp decode_value(4, <<value::32-signed>> <> buffer, :int) do
    {value, buffer}
  end

  defp decode_value(size, buffer, {:list, [type]}) do
    size = size - 4
    <<length::32-signed, content::bytes-size(size)>> <> buffer = buffer
    {decode_list(length, content, type, []), buffer}
  end

  defp decode_value(size, buffer, {:map, [key_type, value_type]}) do
    size = size - 4
    <<count::32-signed, content::bytes-size(size)>> <> buffer = buffer
    {decode_map(count, content, key_type, value_type, []), buffer}
  end

  defp decode_value(size, buffer, {:set, [type]}) do
    size = size - 4
    <<length::32-signed, content::bytes-size(size)>> <> buffer = buffer
    list = decode_list(length, content, type, [])
    {MapSet.new(list), buffer}
  end

  defp decode_value(size, buffer, {:tuple, types}) do
    <<content::bytes-size(size)>> <> buffer = buffer
    {decode_tuple(types, content, []), buffer}
  end

  defp decode_value(size, buffer, :varchar) do
    <<text::size(size)-bytes>> <> buffer = buffer
    {text, buffer}
  end

  defp decode_value(size, buffer, :varint) do
    <<int::size(size)-unit(8), buffer::bytes>> = buffer
    {int, buffer}
  end

  defp decode_value(8, <<value::64-signed>> <> buffer, :timestamp) do
    {value, buffer}
  end

  defp decode_value(16, <<value::16-bytes>> <> buffer, type) when type in [:timeuuid, :uuid] do
    {value, buffer}
  end

  defp decode_list(0, <<>>, _type, acc) do
    Enum.reverse(acc)
  end

  defp decode_list(length, buffer, type, acc) do
    {elem, buffer} = decode_value(buffer, type)
    decode_list(length - 1, buffer, type, [elem | acc])
  end

  defp decode_map(0, <<>>, _key_type, _value_type, acc) do
    Map.new(acc)
  end

  defp decode_map(count, buffer, key_type, value_type, acc) do
    {key, buffer} = decode_value(buffer, key_type)
    {value, buffer} = decode_value(buffer, value_type)
    decode_map(count - 1, buffer, key_type, value_type, [{key, value} | acc])
  end

  def decode_tuple([], <<>>, acc) do
    acc |> Enum.reverse |> List.to_tuple
  end

  def decode_tuple([type | types], buffer, acc) do
    {item, buffer} = decode_value(buffer, type)
    decode_tuple(types, buffer, [item | acc])
  end

  defp decode_column_specs(buffer, 0, _table_spec, acc) do
    {Enum.reverse(acc), buffer}
  end

  defp decode_column_specs(buffer, column_count, nil, acc) do
    {keyspace, buffer} = decode_string(buffer)
    {table, buffer} = decode_string(buffer)
    {name, buffer} = decode_string(buffer)
    {type, buffer} = decode_type(buffer)
    entry = {keyspace, table, name, type}
    decode_column_specs(buffer, column_count - 1, nil, [entry | acc])
  end

  defp decode_column_specs(buffer, column_count, table_spec, acc) do
    {keyspace, table} = table_spec
    {name, buffer} = decode_string(buffer)
    {type, buffer} = decode_type(buffer)
    entry = {keyspace, table, name, type}
    decode_column_specs(buffer, column_count - 1, table_spec, [entry | acc])
  end

  defp decode_type(<<0x0000::16>> <> buffer) do
    {name, buffer} = decode_string(buffer)
    {{:custom, name}, buffer}
  end

  defp decode_type(<<0x0001::16>> <> buffer) do
    {:ascii, buffer}
  end

  defp decode_type(<<0x0002::16>> <> buffer) do
    {:bigint, buffer}
  end

  defp decode_type(<<0x0003::16>> <> buffer) do
    {:blob, buffer}
  end

  defp decode_type(<<0x0004::16>> <> buffer) do
    {:boolean, buffer}
  end

  defp decode_type(<<0x0005::16>> <> buffer) do
    {:counter, buffer}
  end

  defp decode_type(<<0x0006::16>> <> buffer) do
    {:decimal, buffer}
  end

  defp decode_type(<<0x0007::16>> <> buffer) do
    {:double, buffer}
  end

  defp decode_type(<<0x0008::16>> <> buffer) do
    {:float, buffer}
  end

  defp decode_type(<<0x0009::16>> <> buffer) do
    {:int, buffer}
  end

  defp decode_type(<<0x000B::16>> <> buffer) do
    {:timestamp, buffer}
  end

  defp decode_type(<<0x000C::16>> <> buffer) do
    {:uuid, buffer}
  end

  defp decode_type(<<0x000D::16>> <> buffer) do
    {:varchar, buffer}
  end

  defp decode_type(<<0x000E::16>> <> buffer) do
    {:varint, buffer}
  end

  defp decode_type(<<0x000F::16>> <> buffer) do
    {:timeuuid, buffer}
  end

  defp decode_type(<<0x0010::16>> <> buffer) do
    {:inet, buffer}
  end

  defp decode_type(<<0x0020::16>> <> buffer) do
    {type, buffer} = decode_type(buffer)
    {{:list, [type]}, buffer}
  end

  defp decode_type(<<0x0021::16>> <> buffer) do
    {key_type, buffer} = decode_type(buffer)
    {value_type, buffer} = decode_type(buffer)
    {{:map, [key_type, value_type]}, buffer}
  end

  defp decode_type(<<0x0022::16>> <> buffer) do
    {type, buffer} = decode_type(buffer)
    {{:set, [type]}, buffer}
  end

  # TODO: UDT

  defp decode_type(<<0x0031::16, count::16>> <> buffer) do
    decode_type_tuple(count, buffer, [])
  end

  defp decode_type_tuple(0, buffer, acc) do
    {{:tuple, Enum.reverse(acc)}, buffer}
  end

  defp decode_type_tuple(count, buffer, acc) do
    {type, buffer} = decode_type(buffer)
    decode_type_tuple(count - 1, buffer, [type | acc])
  end

  defp decode_string_multimap(<<size::16>> <> buffer) do
    decode_string_multimap(buffer, size, [])
  end

  defp decode_string_multimap(buffer, 0, acc) do
    {Map.new(acc), buffer}
  end

  defp decode_string_multimap(buffer, size, acc) do
    {key, buffer} = decode_string(buffer)
    {value, buffer} = decode_string_list(buffer)
    decode_string_multimap(buffer, size - 1, [{key, value} | acc])
  end

  defp decode_string(<<size::16, content::size(size)-bytes>> <> buffer) do
    {content, buffer}
  end

  defp decode_string_list(<<size::16>> <> buffer) do
    decode_string_list(buffer, size, [])
  end

  defp decode_string_list(buffer, 0, acc) do
    {Enum.reverse(acc), buffer}
  end

  defp decode_string_list(buffer, size, acc) do
    {elem, buffer} = decode_string(buffer)
    decode_string_list(buffer, size - 1, [elem | acc])
  end
end
