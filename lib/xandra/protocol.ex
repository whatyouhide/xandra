defmodule Xandra.Protocol do
  @moduledoc false

  use Bitwise

  alias Xandra.{
    Batch,
    Calendar,
    Error,
    Frame,
    Page,
    Prepared,
    Simple,
    TypeParser
  }

  alias Xandra.Cluster.StatusChange

  @unix_epoch_days 0x80000000

  # We need these two macros to make
  # a single match context possible.

  defmacrop decode_string({:<-, _, [value, buffer]}) do
    quote do
      <<size::16, unquote(value)::size(size)-bytes, unquote(buffer)::bits>> = unquote(buffer)
    end
  end

  defmacrop decode_value({:<-, _, [value, buffer]}, type, do: block) do
    quote do
      <<size::32-signed, unquote(buffer)::bits>> = unquote(buffer)

      if size < 0 do
        unquote(value) = nil
        unquote(block)
      else
        <<data::size(size)-bytes, unquote(buffer)::bits>> = unquote(buffer)
        unquote(value) = decode_value(data, unquote(type))
        unquote(block)
      end
    end
  end

  @spec encode_request(Frame.t(kind), term, Keyword.t()) :: Frame.t(kind) when kind: var
  def encode_request(frame, params, options \\ [])

  def encode_request(%Frame{kind: :options} = frame, nil, _options) do
    %{frame | body: []}
  end

  def encode_request(%Frame{kind: :startup} = frame, requested_options, _options)
      when is_map(requested_options) do
    %{frame | body: encode_string_map(requested_options)}
  end

  def encode_request(%Frame{kind: :auth_response} = frame, _requested_options, options) do
    case Keyword.fetch(options, :authentication) do
      {:ok, authentication} ->
        with {authenticator, auth_options} <- authentication,
             body <- authenticator.response_body(auth_options) do
          %{frame | body: [<<IO.iodata_length(body)::32>>, body]}
        else
          _ ->
            raise "the :authentication option must be " <>
                    "an {auth_module, auth_options} tuple, " <> "got: #{inspect(authentication)}"
        end

      :error ->
        raise "Cassandra server requires authentication but the :authentication option was not provided"
    end
  end

  def encode_request(%Frame{kind: :register} = frame, events, _options) when is_list(events) do
    %{frame | body: encode_string_list(events)}
  end

  def encode_request(%Frame{kind: :query} = frame, %Simple{} = query, options) do
    %{statement: statement, values: values} = query

    body = [
      <<byte_size(statement)::32>>,
      statement,
      encode_params([], values, options, _skip_metadata? = false)
    ]

    %{frame | body: body}
  end

  def encode_request(%Frame{kind: :prepare} = frame, %Prepared{} = prepared, _options) do
    %{statement: statement} = prepared
    %{frame | body: [<<byte_size(statement)::32>>, statement]}
  end

  def encode_request(%Frame{kind: :execute} = frame, %Prepared{} = prepared, options) do
    %{id: id, bound_columns: columns, values: values} = prepared
    skip_metadata? = prepared.result_columns != nil

    body = [
      <<byte_size(id)::16>>,
      id,
      encode_params(columns, values, options, skip_metadata?)
    ]

    %{frame | body: body}
  end

  def encode_request(%Frame{kind: :batch} = frame, %Batch{} = batch, options) do
    %{queries: queries, type: type} = batch

    consistency = Keyword.get(options, :consistency, :one)
    serial_consistency = Keyword.get(options, :serial_consistency)
    timestamp = Keyword.get(options, :timestamp)

    flags =
      0x00
      |> set_flag(_serial_consistency = 0x10, serial_consistency)
      |> set_flag(_default_timestamp = 0x20, timestamp)

    encoded_queries =
      for query <- queries, into: [<<length(queries)::16>>], do: encode_query_in_batch(query)

    body = [
      encode_batch_type(type),
      encoded_queries,
      encode_consistency_level(consistency),
      flags,
      encode_serial_consistency(serial_consistency),
      if(timestamp, do: <<timestamp::64>>, else: [])
    ]

    %{frame | body: body}
  end

  defp encode_batch_type(:logged), do: 0
  defp encode_batch_type(:unlogged), do: 1
  defp encode_batch_type(:counter), do: 2

  defp encode_string_list(list) do
    for string <- list, into: [<<length(list)::16>>] do
      [<<byte_size(string)::16>>, string]
    end
  end

  defp encode_string_map(map) do
    for {key, value} <- map, into: [<<map_size(map)::16>>] do
      [<<byte_size(key)::16>>, key, <<byte_size(value)::16>>, value]
    end
  end

  consistency_levels = %{
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
    0x000A => :local_one
  }

  for {spec, level} <- consistency_levels do
    defp encode_consistency_level(unquote(level)) do
      <<unquote(spec)::16>>
    end
  end

  defp set_flag(mask, bit, value) do
    if value do
      mask ||| bit
    else
      mask
    end
  end

  defp set_query_values_flag(mask, values) do
    cond do
      values == [] or values == %{} ->
        mask

      is_list(values) ->
        mask ||| 0x01

      is_map(values) ->
        mask ||| 0x01 ||| 0x40
    end
  end

  defp encode_params(columns, values, options, skip_metadata?) do
    consistency = Keyword.get(options, :consistency, :one)
    page_size = Keyword.get(options, :page_size, 10_000)
    paging_state = Keyword.get(options, :paging_state)
    serial_consistency = Keyword.get(options, :serial_consistency)
    timestamp = Keyword.get(options, :timestamp)

    flags =
      0x00
      |> set_query_values_flag(values)
      |> set_flag(_page_size = 0x04, true)
      |> set_flag(_metadata_presence = 0x02, skip_metadata?)
      |> set_flag(_paging_state = 0x08, paging_state)
      |> set_flag(_serial_consistency = 0x10, serial_consistency)
      |> set_flag(_default_timestamp = 0x20, timestamp)

    encoded_values =
      if values == [] or values == %{} do
        []
      else
        encode_query_values(columns, values)
      end

    [
      encode_consistency_level(consistency),
      flags,
      encoded_values,
      <<page_size::32>>,
      encode_paging_state(paging_state),
      encode_serial_consistency(serial_consistency),
      if(timestamp, do: <<timestamp::64>>, else: [])
    ]
  end

  defp encode_paging_state(value) do
    if value do
      [<<byte_size(value)::32>>, value]
    else
      []
    end
  end

  defp encode_serial_consistency(nil) do
    []
  end

  defp encode_serial_consistency(consistency) when consistency in [:serial, :local_serial] do
    encode_consistency_level(consistency)
  end

  defp encode_serial_consistency(other) do
    raise ArgumentError,
          "the :serial_consistency option must be either :serial or :local_serial, " <>
            "got: #{inspect(other)}"
  end

  defp encode_query_in_batch(%Simple{statement: statement, values: values}) do
    [
      _kind = 0,
      <<byte_size(statement)::32>>,
      statement,
      encode_query_values([], values)
    ]
  end

  defp encode_query_in_batch(%Prepared{id: id, bound_columns: bound_columns, values: values}) do
    [
      _kind = 1,
      <<byte_size(id)::16>>,
      id,
      encode_query_values(bound_columns, values)
    ]
  end

  defp encode_query_values([], values) when is_list(values) do
    for value <- values, into: [<<length(values)::16>>] do
      encode_query_value(value)
    end
  end

  defp encode_query_values([], values) when is_map(values) do
    for {name, value} <- values, into: [<<map_size(values)::16>>] do
      name = to_string(name)
      [<<byte_size(name)::16>>, name, encode_query_value(value)]
    end
  end

  defp encode_query_values(columns, values) when is_list(values) do
    encode_bound_values(columns, values, [<<length(columns)::16>>])
  end

  defp encode_query_values(columns, values) when map_size(values) == length(columns) do
    for {_keyspace, _table, name, type} <- columns, into: [<<map_size(values)::16>>] do
      value = Map.fetch!(values, name)
      [<<byte_size(name)::16>>, name, encode_query_value(type, value)]
    end
  end

  defp encode_bound_values([], [], acc) do
    acc
  end

  defp encode_bound_values([column | columns], [value | values], acc) do
    {_keyspace, _table, _name, type} = column
    acc = [acc | encode_query_value(type, value)]
    encode_bound_values(columns, values, acc)
  end

  defp encode_query_value({type, value}) when is_binary(type) do
    encode_query_value(TypeParser.parse(type), value)
  end

  defp encode_query_value(_type, nil) do
    <<-1::32>>
  end

  defp encode_query_value(type, value) do
    acc = encode_value(type, value)
    [<<IO.iodata_length(acc)::32>>, acc]
  end

  defp encode_value(:ascii, value) when is_binary(value) do
    value
  end

  defp encode_value(:bigint, value) when is_integer(value) do
    <<value::64>>
  end

  defp encode_value(:blob, value) when is_binary(value) do
    value
  end

  defp encode_value(:boolean, value) do
    case value do
      true -> [1]
      false -> [0]
    end
  end

  defp encode_value(:counter, value) when is_integer(value) do
    <<value::64>>
  end

  defp encode_value(:date, %Date{} = value) do
    value = Calendar.date_to_unix_days(value)
    <<value + @unix_epoch_days::32>>
  end

  defp encode_value(:date, value) when value in -@unix_epoch_days..(@unix_epoch_days - 1) do
    <<value + @unix_epoch_days::32>>
  end

  defp encode_value(:decimal, {value, scale}) do
    [encode_value(:int, scale), encode_value(:varint, value)]
  end

  defp encode_value(:double, value) when is_float(value) do
    <<value::64-float>>
  end

  defp encode_value(:float, value) when is_float(value) do
    <<value::32-float>>
  end

  defp encode_value(:inet, {n1, n2, n3, n4}) do
    <<n1, n2, n3, n4>>
  end

  defp encode_value(:inet, {n1, n2, n3, n4, n5, n6, n7, n8}) do
    <<
      (<<n1::4-bytes, n2::4-bytes, n3::4-bytes, n4::4-bytes>>),
      (<<n5::4-bytes, n6::4-bytes, n7::4-bytes, n8::4-bytes>>)
    >>
  end

  defp encode_value(:int, value) when is_integer(value) do
    <<value::32>>
  end

  defp encode_value({:list, [items_type]}, collection) when is_list(collection) do
    for item <- collection,
        into: [<<length(collection)::32>>],
        do: encode_query_value(items_type, item)
  end

  defp encode_value({:map, [key_type, value_type]}, collection) when is_map(collection) do
    for {key, value} <- collection, into: [<<map_size(collection)::32>>] do
      [
        encode_query_value(key_type, key),
        encode_query_value(value_type, value)
      ]
    end
  end

  defp encode_value({:set, inner_type}, %MapSet{} = collection) do
    encode_value({:list, inner_type}, MapSet.to_list(collection))
  end

  defp encode_value(:smallint, value) when is_integer(value) do
    <<value::16>>
  end

  defp encode_value(:time, %Time{} = time) do
    value = Calendar.time_to_nanoseconds(time)
    <<value::64>>
  end

  defp encode_value(:time, value) when value in 0..86_399_999_999_999 do
    <<value::64>>
  end

  defp encode_value(:timestamp, %DateTime{} = value) do
    <<DateTime.to_unix(value, :milliseconds)::64>>
  end

  defp encode_value(:timestamp, value) when is_integer(value) do
    <<value::64>>
  end

  defp encode_value(:tinyint, value) when is_integer(value) do
    <<value>>
  end

  defp encode_value({:udt, fields}, value) when is_map(value) do
    for {field_name, [field_type]} <- fields do
      encode_query_value(field_type, Map.get(value, field_name))
    end
  end

  defp encode_value(type, value) when type in [:uuid, :timeuuid] and is_binary(value) do
    case byte_size(value) do
      16 ->
        value

      36 ->
        <<
          part1::8-bytes,
          ?-,
          part2::4-bytes,
          ?-,
          part3::4-bytes,
          ?-,
          part4::4-bytes,
          ?-,
          part5::12-bytes
        >> = value

        <<
          decode_base16(part1)::4-bytes,
          decode_base16(part2)::2-bytes,
          decode_base16(part3)::2-bytes,
          decode_base16(part4)::2-bytes,
          decode_base16(part5)::6-bytes
        >>
    end
  end

  defp encode_value(type, value) when type in [:varchar, :text] and is_binary(value) do
    value
  end

  defp encode_value(:varint, value) when is_integer(value) do
    size = varint_byte_size(value)
    <<value::size(size)-unit(8)>>
  end

  defp encode_value({:tuple, types}, value) when length(types) == tuple_size(value) do
    for {type, item} <- Enum.zip(types, Tuple.to_list(value)), do: encode_query_value(type, item)
  end

  defp varint_byte_size(value) when value > 127 do
    1 + varint_byte_size(value >>> 8)
  end

  defp varint_byte_size(value) when value < -128 do
    varint_byte_size(-value - 1)
  end

  defp varint_byte_size(_value), do: 1

  @compile {:inline, decode_base16: 1}
  defp decode_base16(value) do
    Base.decode16!(value, case: :mixed)
  end

  error_codes = %{
    0x0000 => :server_failure,
    0x000A => :protocol_violation,
    0x0100 => :invalid_credentials,
    0x1000 => :unavailable,
    0x1001 => :overloaded,
    0x1002 => :bootstrapping,
    0x1003 => :truncate_failure,
    0x1100 => :write_timeout,
    0x1200 => :read_timeout,
    0x2000 => :invalid_syntax,
    0x2100 => :unauthorized,
    0x2200 => :invalid,
    0x2300 => :invalid_config,
    0x2400 => :already_exists,
    0x2500 => :unprepared
  }

  for {code, reason} <- error_codes do
    defp decode_error_reason(<<unquote(code)::32-signed, buffer::bytes>>) do
      {unquote(reason), buffer}
    end
  end

  defp decode_error_message(_reason, buffer) do
    decode_string(message <- buffer)
    _ = buffer
    message
  end

  @spec decode_response(Frame.t(:error), term) :: Error.t()
  @spec decode_response(Frame.t(:ready), nil) :: :ok
  @spec decode_response(Frame.t(:supported), nil) :: %{optional(String.t()) => [String.t()]}
  @spec decode_response(Frame.t(:result), Simple.t() | Prepared.t() | Batch.t()) ::
          Xandra.result() | Prepared.t()
  def decode_response(frame, query \\ nil, options \\ [])

  def decode_response(%Frame{kind: :error, body: body}, _query, _options) do
    {reason, buffer} = decode_error_reason(body)
    Error.new(reason, decode_error_message(reason, buffer))
  end

  def decode_response(%Frame{kind: :ready, body: <<>>}, nil, _options) do
    :ok
  end

  def decode_response(%Frame{kind: :supported, body: body}, nil, _options) do
    {value, <<>>} = decode_string_multimap(body)
    value
  end

  def decode_response(%Frame{kind: :event, body: body}, nil, _options) do
    decode_string(event <- body)
    "STATUS_CHANGE" = event
    decode_string(effect <- body)
    {address, port, <<>>} = decode_inet(body)
    %StatusChange{effect: effect, address: address, port: port}
  end

  def decode_response(
        %Frame{kind: :result, body: body, atom_keys?: atom_keys?},
        %kind{} = query,
        options
      )
      when kind in [Simple, Prepared, Batch] do
    decode_result_response(body, query, Keyword.put(options, :atom_keys?, atom_keys?))
  end

  defp decode_inet(<<size, data::size(size)-bytes, buffer::bits>>) do
    address = decode_value(data, :inet)
    <<port::32, buffer::bits>> = buffer
    {address, port, buffer}
  end

  # Void
  defp decode_result_response(<<0x0001::32-signed>>, _query, _options) do
    %Xandra.Void{}
  end

  # Page
  defp decode_result_response(<<0x0002::32-signed, buffer::bits>>, query, options) do
    page = new_page(query)
    {page, buffer} = decode_metadata(buffer, page, Keyword.fetch!(options, :atom_keys?))
    columns = rewrite_column_types(page.columns, options)
    %{page | content: decode_page_content(buffer, columns)}
  end

  # SetKeyspace
  defp decode_result_response(<<0x0003::32-signed, buffer::bits>>, _query, _options) do
    decode_string(keyspace <- buffer)
    <<>> = buffer
    %Xandra.SetKeyspace{keyspace: keyspace}
  end

  # Prepared
  defp decode_result_response(
         <<0x0004::32-signed, buffer::bits>>,
         %Prepared{} = prepared,
         options
       ) do
    atom_keys? = Keyword.fetch!(options, :atom_keys?)
    decode_string(id <- buffer)
    {%{columns: bound_columns}, buffer} = decode_metadata(buffer, %Page{}, atom_keys?)
    {%{columns: result_columns}, <<>>} = decode_metadata(buffer, %Page{}, atom_keys?)
    %{prepared | id: id, bound_columns: bound_columns, result_columns: result_columns}
  end

  # SchemaChange
  defp decode_result_response(<<0x0005::32-signed, buffer::bits>>, _query, _options) do
    decode_string(effect <- buffer)
    decode_string(target <- buffer)
    options = decode_change_options(buffer, target)
    %Xandra.SchemaChange{effect: effect, target: target, options: options}
  end

  # Since SELECT statements are not allowed in BATCH queries, there's no need to
  # support %Batch{} in this function.
  defp new_page(%Simple{}), do: %Page{}
  defp new_page(%Prepared{result_columns: result_columns}), do: %Page{columns: result_columns}

  defp rewrite_column_types(columns, options) do
    Enum.map(columns, fn {_, _, _, type} = column ->
      put_elem(column, 3, rewrite_type(type, options))
    end)
  end

  defp rewrite_type({parent_type, types}, options) do
    {parent_type, Enum.map(types, &rewrite_type(&1, options))}
  end

  defp rewrite_type(:date, options) do
    {:date, [Keyword.get(options, :date_format, :date)]}
  end

  defp rewrite_type(:time, options) do
    {:time, [Keyword.get(options, :time_format, :time)]}
  end

  defp rewrite_type(:timestamp, options) do
    {:timestamp, [Keyword.get(options, :timestamp_format, :datetime)]}
  end

  defp rewrite_type(type, _options), do: type

  defp decode_change_options(<<buffer::bits>>, "KEYSPACE") do
    decode_string(keyspace <- buffer)
    <<>> = buffer
    %{keyspace: keyspace}
  end

  defp decode_change_options(<<buffer::bits>>, target) when target in ["TABLE", "TYPE"] do
    decode_string(keyspace <- buffer)
    decode_string(subject <- buffer)
    <<>> = buffer
    %{keyspace: keyspace, subject: subject}
  end

  defp decode_metadata(
         <<flags::4-bytes, column_count::32-signed, buffer::bits>>,
         page,
         atom_keys?
       ) do
    <<_::29, no_metadata::1, has_more_pages::1, global_table_spec::1>> = flags
    {page, buffer} = decode_paging_state(buffer, page, has_more_pages)

    cond do
      no_metadata == 1 ->
        {page, buffer}

      global_table_spec == 1 ->
        decode_string(keyspace <- buffer)
        decode_string(table <- buffer)

        {columns, buffer} =
          decode_columns(buffer, column_count, {keyspace, table}, atom_keys?, [])

        {%{page | columns: columns}, buffer}

      true ->
        {columns, buffer} = decode_columns(buffer, column_count, nil, atom_keys?, [])
        {%{page | columns: columns}, buffer}
    end
  end

  defp decode_paging_state(<<buffer::bits>>, page, 0) do
    {page, buffer}
  end

  defp decode_paging_state(<<buffer::bits>>, page, 1) do
    <<size::32, paging_state::size(size)-bytes, buffer::bits>> = buffer
    {%{page | paging_state: paging_state}, buffer}
  end

  defp decode_page_content(<<row_count::32-signed, buffer::bits>>, columns) do
    decode_page_content(buffer, row_count, columns, columns, [[]])
  end

  defp decode_page_content(<<>>, 0, columns, columns, [[] | acc]) do
    Enum.reverse(acc)
  end

  defp decode_page_content(<<buffer::bits>>, row_count, columns, [], [values | acc]) do
    decode_page_content(buffer, row_count - 1, columns, columns, [[], Enum.reverse(values) | acc])
  end

  defp decode_page_content(<<buffer::bits>>, row_count, columns, [{_, _, _, type} | rest], [
         values | acc
       ]) do
    decode_value(value <- buffer, type) do
      values = [value | values]
      decode_page_content(buffer, row_count, columns, rest, [values | acc])
    end
  end

  defp decode_value(<<value>>, :boolean), do: value != 0
  defp decode_value(<<value::signed>>, :tinyint), do: value
  defp decode_value(<<value::16-signed>>, :smallint), do: value

  defp decode_value(<<value::64>>, {:time, [format]}) do
    case format do
      :time -> Calendar.time_from_nanoseconds(value)
      :integer -> value
    end
  end

  defp decode_value(<<value::64-signed>>, :bigint), do: value
  defp decode_value(<<value::64-signed>>, :counter), do: value

  defp decode_value(<<value::64-signed>>, {:timestamp, [format]}) do
    case format do
      :datetime -> DateTime.from_unix!(value, :milliseconds)
      :integer -> value
    end
  end

  defp decode_value(<<value::32>>, {:date, [format]}) do
    unix_days = value - @unix_epoch_days

    case format do
      :date -> Calendar.date_from_unix_days(unix_days)
      :integer -> unix_days
    end
  end

  defp decode_value(<<value::32-signed>>, :int), do: value
  defp decode_value(<<value::64-float>>, :double), do: value
  defp decode_value(<<value::32-float>>, :float), do: value

  defp decode_value(<<data::4-bytes>>, :inet) do
    <<n1, n2, n3, n4>> = data
    {n1, n2, n3, n4}
  end

  defp decode_value(<<data::16-bytes>>, :inet) do
    <<n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16>> = data
    {n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16}
  end

  defp decode_value(<<value::16-bytes>>, :timeuuid), do: value
  defp decode_value(<<value::16-bytes>>, :uuid), do: value

  defp decode_value(<<scale::32-signed, data::bits>>, :decimal) do
    {decode_value(data, :varint), scale}
  end

  defp decode_value(<<count::32-signed, data::bits>>, {:list, [type]}) do
    decode_value_list(data, count, type, [])
  end

  defp decode_value(<<count::32-signed, data::bits>>, {:map, [key_type, value_type]}) do
    decode_value_map_key(data, count, key_type, value_type, [])
  end

  defp decode_value(<<count::32-signed, data::bits>>, {:set, [type]}) do
    data
    |> decode_value_list(count, type, [])
    |> MapSet.new()
  end

  defp decode_value(<<value::bits>>, :ascii), do: value
  defp decode_value(<<value::bits>>, :blob), do: value
  defp decode_value(<<value::bits>>, :varchar), do: value

  # For legacy compatibility reasons, most non-string types support
  # "empty" values (that is a value with zero length).
  # An empty value is distinct from NULL, which is encoded with a negative length.
  defp decode_value(<<>>, _type), do: nil

  defp decode_value(<<data::bits>>, {:udt, fields}) do
    decode_value_udt(data, fields, [])
  end

  defp decode_value(<<data::bits>>, {:tuple, types}) do
    decode_value_tuple(data, types, [])
  end

  defp decode_value(<<data::bits>>, :varint) do
    size = bit_size(data)
    <<value::size(size)-signed>> = data
    value
  end

  defp decode_value_udt(<<>>, fields, acc) do
    for {field_name, _} <- fields, into: Map.new(acc), do: {field_name, nil}
  end

  defp decode_value_udt(<<buffer::bits>>, [{field_name, [field_type]} | rest], acc) do
    decode_value(value <- buffer, field_type) do
      decode_value_udt(buffer, rest, [{field_name, value} | acc])
    end
  end

  defp decode_value_list(<<>>, 0, _type, acc) do
    Enum.reverse(acc)
  end

  defp decode_value_list(<<buffer::bits>>, count, type, acc) do
    decode_value(item <- buffer, type) do
      decode_value_list(buffer, count - 1, type, [item | acc])
    end
  end

  defp decode_value_map_key(<<>>, 0, _key_type, _value_type, acc) do
    Map.new(acc)
  end

  defp decode_value_map_key(<<buffer::bits>>, count, key_type, value_type, acc) do
    decode_value(key <- buffer, key_type) do
      decode_value_map_value(buffer, count, key_type, value_type, [key | acc])
    end
  end

  defp decode_value_map_value(<<buffer::bits>>, count, key_type, value_type, [key | acc]) do
    decode_value(value <- buffer, value_type) do
      decode_value_map_key(buffer, count - 1, key_type, value_type, [{key, value} | acc])
    end
  end

  defp decode_value_tuple(<<buffer::bits>>, [type | types], acc) do
    decode_value(item <- buffer, type) do
      decode_value_tuple(buffer, types, [item | acc])
    end
  end

  defp decode_value_tuple(<<>>, [], acc) do
    acc |> Enum.reverse() |> List.to_tuple()
  end

  defp decode_columns(<<buffer::bits>>, 0, _table_spec, _atom_keys?, acc) do
    {Enum.reverse(acc), buffer}
  end

  defp decode_columns(<<buffer::bits>>, column_count, nil, atom_keys?, acc) do
    decode_string(keyspace <- buffer)
    decode_string(table <- buffer)
    decode_string(name <- buffer)
    name = if atom_keys?, do: String.to_atom(name), else: name
    {type, buffer} = decode_type(buffer)
    entry = {keyspace, table, name, type}
    decode_columns(buffer, column_count - 1, nil, atom_keys?, [entry | acc])
  end

  defp decode_columns(<<buffer::bits>>, column_count, table_spec, atom_keys?, acc) do
    {keyspace, table} = table_spec
    decode_string(name <- buffer)
    name = if atom_keys?, do: String.to_atom(name), else: name
    {type, buffer} = decode_type(buffer)
    entry = {keyspace, table, name, type}
    decode_columns(buffer, column_count - 1, table_spec, atom_keys?, [entry | acc])
  end

  defp decode_type(<<0x0000::16, buffer::bits>>) do
    decode_string(class <- buffer)
    {custom_type_to_native(class), buffer}
  end

  defp decode_type(<<0x0001::16, buffer::bits>>) do
    {:ascii, buffer}
  end

  defp decode_type(<<0x0002::16, buffer::bits>>) do
    {:bigint, buffer}
  end

  defp decode_type(<<0x0003::16, buffer::bits>>) do
    {:blob, buffer}
  end

  defp decode_type(<<0x0004::16, buffer::bits>>) do
    {:boolean, buffer}
  end

  defp decode_type(<<0x0005::16, buffer::bits>>) do
    {:counter, buffer}
  end

  defp decode_type(<<0x0006::16, buffer::bits>>) do
    {:decimal, buffer}
  end

  defp decode_type(<<0x0007::16, buffer::bits>>) do
    {:double, buffer}
  end

  defp decode_type(<<0x0008::16, buffer::bits>>) do
    {:float, buffer}
  end

  defp decode_type(<<0x0009::16, buffer::bits>>) do
    {:int, buffer}
  end

  defp decode_type(<<0x000B::16, buffer::bits>>) do
    {:timestamp, buffer}
  end

  defp decode_type(<<0x000C::16, buffer::bits>>) do
    {:uuid, buffer}
  end

  defp decode_type(<<0x000D::16, buffer::bits>>) do
    {:varchar, buffer}
  end

  defp decode_type(<<0x000E::16, buffer::bits>>) do
    {:varint, buffer}
  end

  defp decode_type(<<0x000F::16, buffer::bits>>) do
    {:timeuuid, buffer}
  end

  defp decode_type(<<0x0010::16, buffer::bits>>) do
    {:inet, buffer}
  end

  defp decode_type(<<0x0020::16, buffer::bits>>) do
    {type, buffer} = decode_type(buffer)
    {{:list, [type]}, buffer}
  end

  defp decode_type(<<0x0021::16, buffer::bits>>) do
    {key_type, buffer} = decode_type(buffer)
    {value_type, buffer} = decode_type(buffer)
    {{:map, [key_type, value_type]}, buffer}
  end

  defp decode_type(<<0x0022::16, buffer::bits>>) do
    {type, buffer} = decode_type(buffer)
    {{:set, [type]}, buffer}
  end

  defp decode_type(<<0x0030::16, buffer::bits>>) do
    decode_string(_keyspace <- buffer)
    decode_string(_name <- buffer)
    <<count::16, buffer::bits>> = buffer
    decode_type_udt(buffer, count, [])
  end

  defp decode_type(<<0x0031::16, count::16, buffer::bits>>) do
    decode_type_tuple(buffer, count, [])
  end

  custom_types = %{
    "org.apache.cassandra.db.marshal.SimpleDateType" => :date,
    "org.apache.cassandra.db.marshal.ShortType" => :smallint,
    "org.apache.cassandra.db.marshal.ByteType" => :tinyint,
    "org.apache.cassandra.db.marshal.TimeType" => :time
  }

  for {class, type} <- custom_types do
    defp custom_type_to_native(unquote(class)) do
      unquote(type)
    end
  end

  defp custom_type_to_native(class) do
    raise "cannot decode custom type #{inspect(class)}"
  end

  defp decode_type_udt(<<buffer::bits>>, 0, acc) do
    {{:udt, Enum.reverse(acc)}, buffer}
  end

  defp decode_type_udt(<<buffer::bits>>, count, acc) do
    decode_string(field_name <- buffer)
    {field_type, buffer} = decode_type(buffer)
    decode_type_udt(buffer, count - 1, [{field_name, [field_type]} | acc])
  end

  defp decode_type_tuple(<<buffer::bits>>, 0, acc) do
    {{:tuple, Enum.reverse(acc)}, buffer}
  end

  defp decode_type_tuple(<<buffer::bits>>, count, acc) do
    {type, buffer} = decode_type(buffer)
    decode_type_tuple(buffer, count - 1, [type | acc])
  end

  defp decode_string_multimap(<<count::16, buffer::bits>>) do
    decode_string_multimap(buffer, count, [])
  end

  defp decode_string_multimap(<<buffer::bits>>, 0, acc) do
    {Map.new(acc), buffer}
  end

  defp decode_string_multimap(<<buffer::bits>>, count, acc) do
    decode_string(key <- buffer)
    {value, buffer} = decode_string_list(buffer)
    decode_string_multimap(buffer, count - 1, [{key, value} | acc])
  end

  defp decode_string_list(<<count::16, buffer::bits>>) do
    decode_string_list(buffer, count, [])
  end

  defp decode_string_list(<<buffer::bits>>, 0, acc) do
    {Enum.reverse(acc), buffer}
  end

  defp decode_string_list(<<buffer::bits>>, count, acc) do
    decode_string(item <- buffer)
    decode_string_list(buffer, count - 1, [item | acc])
  end
end
