defmodule Xandra.Connection do
  @moduledoc false

  use DBConnection

  alias Xandra.{Batch, ConnectionError, Prepared, Frame, Simple, SetKeyspace}
  alias __MODULE__.Utils

  @default_timeout 5_000
  @forced_transport_options [packet: :raw, mode: :binary, active: false]

  defstruct [
    :transport,
    :transport_options,
    :socket,
    :prepared_cache,
    :compressor,
    :default_consistency,
    :atom_keys?,
    :protocol_module,
    :current_keyspace,
    :address,
    :port,
    :connection_name,
    :registry,
    :pool_index,
    :peername
  ]

  @impl true
  def connect(options) when is_list(options) do
    {address, port} = Keyword.fetch!(options, :node)
    compressor = Keyword.get(options, :compressor)
    enforced_protocol = Keyword.get(options, :protocol_version)
    transport = if(options[:encryption], do: :ssl, else: :gen_tcp)
    connection_name = options[:name]

    transport_options =
      options
      |> Keyword.get(:transport_options, [])
      |> Keyword.merge(@forced_transport_options)

    case transport.connect(address, port, transport_options, @default_timeout) do
      {:ok, socket} ->
        {:ok, peername} = inet_mod(transport).peername(socket)

        state = %__MODULE__{
          transport: transport,
          transport_options: transport_options,
          socket: socket,
          prepared_cache: Keyword.fetch!(options, :prepared_cache),
          compressor: compressor,
          default_consistency: Keyword.fetch!(options, :default_consistency),
          atom_keys?: Keyword.fetch!(options, :atom_keys),
          current_keyspace: nil,
          address: address,
          port: port,
          connection_name: connection_name,
          registry: Keyword.get(options, :registry),
          pool_index: Keyword.fetch!(options, :pool_index),
          peername: peername
        }

        with {:ok, supported_options, protocol_module} <-
               Utils.request_options(transport, socket, enforced_protocol),
             state = %__MODULE__{state | protocol_module: protocol_module},
             :ok <-
               startup_connection(
                 transport,
                 socket,
                 supported_options,
                 protocol_module,
                 compressor,
                 options
               ) do
          :telemetry.execute([:xandra, :connected], %{}, %{
            connection_name: connection_name,
            address: address,
            port: port
          })

          maybe_register_or_put_value(state, :up)
          {:ok, state}
        else
          {:error, {:unsupported_protocol, protocol_version}} ->
            raise """
            native protocol version negotiation with the server failed. The server \
            wants to use protocol #{inspect(protocol_version)}, but Xandra only \
            supports these protocols: #{inspect(Frame.supported_protocols())}\
            """

          {:error, {:use_this_protocol_instead, failed_protocol_version, protocol_version}} ->
            :telemetry.execute([:xandra, :debug, :downgrading_protocol], %{}, %{
              failed_version: failed_protocol_version,
              new_version: protocol_version,
              address: address,
              port: port
            })

            :ok = transport.close(socket)
            options = Keyword.put(options, :protocol_version, protocol_version)
            connect(options)

          {:error, %Xandra.Error{} = error} ->
            raise error

          {:error, reason} = error ->
            disconnect(reason, state)
            error
        end

      {:error, reason} ->
        message = if transport == :ssl, do: "TLS/SSL connect", else: "TCP connect"
        {:error, ConnectionError.new(message, reason)}
    end
  end

  @impl true
  def handle_begin(_opts, _state) do
    raise ArgumentError, "Cassandra doesn't support transactions"
  end

  @impl true
  def handle_commit(_opts, _state) do
    raise ArgumentError, "Cassandra doesn't support transactions"
  end

  @impl true
  def handle_rollback(_opts, _state) do
    raise ArgumentError, "Cassandra doesn't support transactions"
  end

  @impl true
  def handle_status(_opts, state) do
    # :idle means we're not in a transaction.
    {:idle, state}
  end

  @impl true
  def handle_declare(_query, _params, _opts, _state) do
    raise ArgumentError, "Cassandra doesn't support cursors"
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, _state) do
    raise ArgumentError, "Cassandra doesn't support cursors"
  end

  @impl true
  def handle_fetch(_query, _cursor, _opts, _state) do
    raise ArgumentError, "Cassandra doesn't support cursors"
  end

  @impl true
  def checkout(state) do
    {:ok, state}
  end

  @impl true
  def handle_prepare(%Prepared{} = prepared, options, %__MODULE__{} = state) do
    compressor = get_right_compressor(state, options[:compressor])

    prepared = %Prepared{
      prepared
      | default_consistency: state.default_consistency,
        protocol_module: state.protocol_module,
        keyspace: state.current_keyspace,
        compressor: compressor,
        request_custom_payload: options[:custom_payload]
    }

    force? = Keyword.fetch!(options, :force)

    telemetry_metadata = Keyword.fetch!(options, :telemetry_metadata)

    metadata = %{
      query: prepared,
      connection_name: state.connection_name,
      address: state.address,
      port: state.port,
      extra_metadata: telemetry_metadata
    }

    case prepared_cache_lookup(state, prepared, force?) do
      {:ok, prepared} ->
        :telemetry.execute([:xandra, :prepared_cache, :hit], %{}, metadata)
        {:ok, prepared, state}

      {:error, cache_status} ->
        :telemetry.execute([:xandra, :prepared_cache, cache_status], %{}, metadata)

        :telemetry.span(
          [:xandra, :prepare_query],
          metadata,
          fn ->
            case send_prepared(prepared, options, state) do
              {:ok, prepared, state} ->
                reprepared = cache_status == :hit
                {{:ok, prepared, state}, Map.put(metadata, :reprepared, reprepared)}

              {:error, %Xandra.Error{} = error, state} ->
                {{:error, error, state}, Map.put(metadata, :reason, error)}

              {:disconnect, reason, state} ->
                {{:disconnect, reason, state}, Map.put(metadata, :reason, reason)}
            end
          end
        )
    end
  end

  def handle_prepare(%Simple{} = simple, options, state) do
    simple = %Simple{
      simple
      | default_consistency: state.default_consistency,
        protocol_module: state.protocol_module,
        compressor: get_right_compressor(state, options[:compressor]),
        custom_payload: options[:custom_payload]
    }

    {:ok, simple, state}
  end

  def handle_prepare(%Batch{} = batch, options, state) do
    batch = %Batch{
      batch
      | default_consistency: state.default_consistency,
        protocol_module: state.protocol_module,
        compressor: get_right_compressor(state, options[:compressor]),
        custom_payload: options[:custom_payload]
    }

    {:ok, batch, state}
  end

  defp send_prepared(%Prepared{compressor: compressor} = prepared, options, state) do
    transport = state.transport

    frame_options =
      options
      |> Keyword.take([:tracing, :custom_payload])
      |> Keyword.put(:compressor, compressor)

    payload =
      Frame.new(:prepare, frame_options)
      |> state.protocol_module.encode_request(prepared)
      |> Frame.encode(state.protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(state.protocol_module)

    with :ok <- transport.send(state.socket, payload),
         {:ok, %Frame{} = frame, rest} <-
           Utils.recv_frame(transport, state.socket, protocol_format, state.compressor) do
      "" = rest
      frame = %Frame{frame | atom_keys?: state.atom_keys?}

      case state.protocol_module.decode_response(frame, prepared) do
        {%Prepared{} = prepared, warnings} ->
          maybe_execute_telemetry_event_for_warnings(state, prepared, warnings)
          Prepared.Cache.insert(state.prepared_cache, prepared)
          {:ok, prepared, state}

        %Xandra.Error{} = reason ->
          {:error, reason, state}
      end
    else
      {:error, reason} ->
        {:disconnect, ConnectionError.new("prepare", reason), state}
    end
  end

  @impl true
  def handle_execute(query, payload, options, %__MODULE__{} = state) do
    assert_valid_compressor(state.compressor, options[:compressor])

    telemetry_metadata = Keyword.fetch!(options, :telemetry_metadata)

    metadata = %{
      query: query,
      connection_name: state.connection_name,
      address: state.address,
      port: state.port,
      extra_metadata: telemetry_metadata
    }

    :telemetry.span(
      [:xandra, :execute_query],
      metadata,
      fn ->
        case send_query(query, payload, options, state) do
          {:ok, response, state} ->
            {{:ok, query, response, state}, metadata}

          {:error, %Xandra.Error{} = error, state} ->
            {{:ok, query, error, state}, Map.put(metadata, :reason, error)}

          {:disconnect, reason, state} ->
            {{:disconnect, reason, state}, Map.put(metadata, :reason, reason)}
        end
      end
    )
  end

  defp send_query(query, payload, options, %__MODULE__{} = state) do
    protocol_format = Xandra.Protocol.frame_protocol_format(state.protocol_module)

    with :ok <- state.transport.send(state.socket, payload),
         {:ok, %Frame{} = frame, rest} <-
           Utils.recv_frame(state.transport, state.socket, protocol_format, state.compressor) do
      "" = rest
      frame = %Frame{frame | atom_keys?: state.atom_keys?}

      case state.protocol_module.decode_response(frame, query, options) do
        {%_{} = response, warnings} ->
          maybe_execute_telemetry_event_for_warnings(state, query, warnings)

          state =
            case response do
              %SetKeyspace{keyspace: keyspace} -> %__MODULE__{state | current_keyspace: keyspace}
              _other -> state
            end

          {:ok, response, state}

        %Xandra.Error{} = error ->
          {:error, error, state}
      end
    else
      {:error, reason} ->
        {:disconnect, ConnectionError.new("execute", reason), state}
    end
  end

  defp maybe_execute_telemetry_event_for_warnings(%__MODULE__{} = state, query, warnings) do
    if warnings != [] do
      metadata =
        state
        |> Map.take([:address, :port, :current_keyspace])
        |> Map.put(:query, query)

      :telemetry.execute([:xandra, :server_warnings], %{warnings: warnings}, metadata)
    end
  end

  @impl true
  def handle_close(query, _options, state) do
    {:ok, query, state}
  end

  @impl true
  def disconnect(exception, %__MODULE__{transport: transport, socket: socket} = state) do
    :telemetry.execute([:xandra, :disconnected], %{}, %{
      connection: self(),
      connection_name: state.connection_name,
      address: state.address,
      port: state.port,
      reason: exception
    })

    maybe_register_or_put_value(state, :down)
    :ok = transport.close(socket)
  end

  @impl true
  def ping(%__MODULE__{socket: socket, compressor: compressor} = state) do
    case Utils.ping(state.transport, socket, state.protocol_module, compressor) do
      :ok ->
        {:ok, state}

      {:error, %Xandra.Error{} = error} ->
        raise "unexpected Cassandra error when requesting options just to \"ping\" the " <>
                "connection: #{Exception.message(error)}"

      {:error, %ConnectionError{reason: reason}} ->
        {:disconnect, ConnectionError.new("ping", reason), state}
    end
  end

  defp prepared_cache_lookup(state, prepared, true) do
    cache_status =
      case Prepared.Cache.lookup(state.prepared_cache, prepared) do
        {:ok, %Prepared{}} -> :hit
        :error -> :miss
      end

    Prepared.Cache.delete(state.prepared_cache, prepared)
    {:error, cache_status}
  end

  defp prepared_cache_lookup(state, prepared, false) do
    case Prepared.Cache.lookup(state.prepared_cache, prepared) do
      {:ok, prepared} -> {:ok, prepared}
      :error -> {:error, :miss}
    end
  end

  defp startup_connection(
         transport,
         socket,
         supported_options,
         protocol_module,
         compressor,
         options
       ) do
    %{
      "CQL_VERSION" => [cql_version | _],
      "COMPRESSION" => supported_compression_algorithms
    } = supported_options

    requested_options = %{"CQL_VERSION" => cql_version}

    if compressor do
      compression_algorithm = Atom.to_string(compressor.algorithm())

      if compression_algorithm in supported_compression_algorithms do
        requested_options = Map.put(requested_options, "COMPRESSION", compression_algorithm)

        Utils.startup_connection(
          transport,
          socket,
          requested_options,
          protocol_module,
          compressor,
          options
        )
      else
        {:error,
         ConnectionError.new(
           "startup connection",
           {:unsupported_compression, compressor.algorithm()}
         )}
      end
    else
      Utils.startup_connection(
        transport,
        socket,
        requested_options,
        protocol_module,
        compressor,
        options
      )
    end
  end

  defp maybe_register_or_put_value(%__MODULE__{registry: nil}, _value) do
    :ok
  end

  defp maybe_register_or_put_value(%__MODULE__{peername: {address, port}} = state, value)
       when is_tuple(address) do
    key = {{address, port}, state.pool_index}

    case Registry.register(state.registry, key, value) do
      {:ok, _owner} ->
        :ok

      {:error, {:already_registered, _}} ->
        {_new, _old} = Registry.update_value(state.registry, key, fn _ -> value end)
    end
  end

  defp get_right_compressor(%__MODULE__{} = state, provided) do
    case Xandra.Protocol.frame_protocol_format(state.protocol_module) do
      :v5_or_more -> assert_valid_compressor(state.compressor, provided) || state.compressor
      :v4_or_less -> assert_valid_compressor(state.compressor, provided)
    end
  end

  # If the user doesn't provide a compression module, it's fine because we don't
  # compress the outgoing frame (but we decompress the incoming frame).
  defp assert_valid_compressor(_initial, _provided = nil) do
    nil
  end

  # If this connection wasn't started with compression set up but the user
  # provides a compressor module, we blow up because it is a semantic error.
  defp assert_valid_compressor(_initial = nil, provided) do
    raise ArgumentError,
          "a query was compressed with the #{inspect(provided)} compressor module " <>
            "but the connection was started without specifying any compression"
  end

  # If the user provided a compressor module both for this prepare/execute as
  # well as when starting the connection, then we check that the compression
  # algorithm of both is the same (potentially, they can use different
  # compressor modules provided they use the same algorithm), and if not then
  # this is a semantic error so we blow up.
  defp assert_valid_compressor(initial, provided) do
    initial_algorithm = initial.algorithm()
    provided_algorithm = provided.algorithm()

    if initial_algorithm == provided_algorithm do
      provided
    else
      raise ArgumentError,
            "a query was compressed with the #{inspect(provided)} compressor module " <>
              "(which uses the #{inspect(provided_algorithm)} algorithm) but the " <>
              "connection was initialized with the #{inspect(initial)} compressor " <>
              "module (which uses the #{inspect(initial_algorithm)} algorithm)"
    end
  end

  defp inet_mod(:gen_tcp), do: :inet
  defp inet_mod(:ssl), do: :ssl
end
