defmodule Xandra.Connection do
  @moduledoc false

  use DBConnection

  alias Xandra.{Batch, ConnectionError, Prepared, Frame, Simple, SetKeyspace}
  alias __MODULE__.Utils

  require Logger

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
    :current_keyspace
  ]

  @impl true
  def connect(options) when is_list(options) do
    {address, port} = Keyword.fetch!(options, :node)
    prepared_cache = Keyword.fetch!(options, :prepared_cache)
    compressor = Keyword.get(options, :compressor)
    default_consistency = Keyword.fetch!(options, :default_consistency)
    atom_keys? = Keyword.get(options, :atom_keys, false)
    enforced_protocol = Keyword.get(options, :protocol_version)
    transport = if(options[:encryption], do: :ssl, else: :gen_tcp)

    transport_options =
      options
      |> Keyword.get(:transport_options, [])
      |> Keyword.merge(@forced_transport_options)

    # Set the logger metadata for the whole process.
    :ok = Logger.metadata(xandra_address: inspect(address), xandra_port: port)

    case transport.connect(address, port, transport_options, @default_timeout) do
      {:ok, socket} ->
        state = %__MODULE__{
          transport: transport,
          transport_options: transport_options,
          socket: socket,
          prepared_cache: prepared_cache,
          compressor: compressor,
          default_consistency: default_consistency,
          atom_keys?: atom_keys?,
          current_keyspace: nil
        }

        with {:ok, supported_options, protocol_module} <-
               Utils.request_options(transport, socket, enforced_protocol),
             state = %__MODULE__{state | protocol_module: protocol_module},
             Logger.metadata(xandra_protocol_module: state.protocol_module),
             Logger.debug("Connected successfully, using protocol #{inspect(protocol_module)}"),
             Logger.debug("Supported options: #{inspect(supported_options)}"),
             :ok <-
               startup_connection(
                 transport,
                 socket,
                 supported_options,
                 protocol_module,
                 compressor,
                 options
               ) do
          {:ok, state}
        else
          {:error, {:unsupported_protocol, protocol_version}} ->
            raise """
            native protocol version negotiation with the server failed. The server \
            wants to use protocol #{inspect(protocol_version)}, but Xandra only \
            supports these protocols: #{inspect(Frame.supported_protocols())}\
            """

          {:error, {:use_this_protocol_instead, protocol_version}} ->
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
        {:error, ConnectionError.new("TCP connect", reason)}
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
  def handle_prepare(%Prepared{} = prepared, options, %__MODULE__{socket: socket} = state) do
    prepared = %Prepared{
      prepared
      | default_consistency: state.default_consistency,
        protocol_module: state.protocol_module,
        keyspace: state.current_keyspace
    }

    force? = Keyword.fetch!(options, :force)
    compressor = assert_valid_compressor(state.compressor, options[:compressor])
    transport = state.transport

    case prepared_cache_lookup(state, prepared, force?) do
      {:ok, prepared} ->
        {:ok, prepared, state}

      :error ->
        frame_options =
          options |> Keyword.take([:tracing]) |> Keyword.put(:compressor, compressor)

        payload =
          Frame.new(:prepare, frame_options)
          |> state.protocol_module.encode_request(prepared)
          |> Frame.encode(state.protocol_module)

        protocol_format =
          case state.protocol_module do
            Xandra.Protocol.V5 -> :v5_or_more
            _other -> :v4_or_less
          end

        with :ok <- transport.send(socket, payload),
             {:ok, %Frame{} = frame} <-
               Utils.recv_frame(transport, socket, protocol_format, state.compressor),
             frame = %{frame | atom_keys?: state.atom_keys?},
             %Prepared{} = prepared <- state.protocol_module.decode_response(frame, prepared) do
          Prepared.Cache.insert(state.prepared_cache, prepared)
          {:ok, prepared, state}
        else
          {:error, reason} ->
            {:disconnect, ConnectionError.new("prepare", reason), state}

          %Xandra.Error{} = reason ->
            {:error, reason, state}
        end
    end
  end

  def handle_prepare(%Simple{} = simple, _options, state) do
    simple = %{
      simple
      | default_consistency: state.default_consistency,
        protocol_module: state.protocol_module
    }

    {:ok, simple, state}
  end

  def handle_prepare(%Batch{} = batch, _options, state) do
    batch = %{
      batch
      | default_consistency: state.default_consistency,
        protocol_module: state.protocol_module
    }

    {:ok, batch, state}
  end

  @impl true
  def handle_execute(query, payload, options, %__MODULE__{} = state) do
    %{socket: socket, compressor: compressor, atom_keys?: atom_keys?} = state
    assert_valid_compressor(compressor, options[:compressor])

    protocol_format =
      case state.protocol_module do
        Xandra.Protocol.V5 -> :v5_or_more
        _other -> :v4_or_less
      end

    with :ok <- state.transport.send(socket, payload),
         {:ok, %Frame{} = frame} <-
           Utils.recv_frame(state.transport, socket, protocol_format, compressor),
         frame = %{frame | atom_keys?: atom_keys?},
         %SetKeyspace{keyspace: keyspace} = response <-
           state.protocol_module.decode_response(frame, query, options) do
      {:ok, query, response, %{state | current_keyspace: keyspace}}
    else
      %_{} = response ->
        {:ok, query, response, state}

      {:error, reason} ->
        {:disconnect, ConnectionError.new("execute", reason), state}
    end
  end

  @impl true
  def handle_close(query, _options, state) do
    {:ok, query, state}
  end

  @impl true
  def disconnect(_exception, %__MODULE__{transport: transport, socket: socket}) do
    :ok = transport.close(socket)
  end

  @impl true
  def ping(%__MODULE__{socket: socket, compressor: compressor} = state) do
    case Utils.request_options(state.transport, socket, state.protocol_module, compressor) do
      {:ok, _options} ->
        {:ok, state}

      {:error, %ConnectionError{reason: reason}} ->
        {:disconnect, ConnectionError.new("ping", reason), state}
    end
  end

  defp prepared_cache_lookup(state, prepared, true) do
    Prepared.Cache.delete(state.prepared_cache, prepared)
    :error
  end

  defp prepared_cache_lookup(state, prepared, false) do
    Prepared.Cache.lookup(state.prepared_cache, prepared)
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
end
