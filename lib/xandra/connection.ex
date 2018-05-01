defmodule Xandra.Connection do
  @moduledoc false

  use DBConnection

  alias Xandra.{ConnectionError, Prepared, Frame, Protocol}
  alias __MODULE__.Utils

  @default_timeout 5_000
  @default_socket_options [packet: :raw, mode: :binary, active: false]

  defstruct [:socket, :prepared_cache, :compressor, :atom_keys?]

  def connect(options) do
    address = Keyword.fetch!(options, :address)
    port = Keyword.fetch!(options, :port)
    prepared_cache = Keyword.fetch!(options, :prepared_cache)
    compressor = Keyword.get(options, :compressor)
    atom_keys? = Keyword.get(options, :atom_keys, false)

    case :gen_tcp.connect(address, port, @default_socket_options, @default_timeout) do
      {:ok, socket} ->
        state = %__MODULE__{
          socket: socket,
          prepared_cache: prepared_cache,
          compressor: compressor,
          atom_keys?: atom_keys?
        }

        with {:ok, supported_options} <- Utils.request_options(socket),
             :ok <- startup_connection(socket, supported_options, compressor, options) do
          {:ok, state}
        else
          {:error, reason} = error ->
            disconnect(reason, state)
            error
        end

      {:error, reason} ->
        {:error, ConnectionError.new("TCP connect", reason)}
    end
  end

  def checkout(state) do
    {:ok, state}
  end

  def checkin(state) do
    {:ok, state}
  end

  def handle_prepare(%Prepared{} = prepared, options, %__MODULE__{socket: socket} = state) do
    force? = Keyword.get(options, :force, false)
    compressor = assert_valid_compressor(state.compressor, options[:compressor])

    case prepared_cache_lookup(state, prepared, force?) do
      {:ok, prepared} ->
        {:ok, prepared, state}

      :error ->
        payload =
          Frame.new(:prepare)
          |> Protocol.encode_request(prepared)
          |> Frame.encode(compressor)

        with :ok <- :gen_tcp.send(socket, payload),
             {:ok, %Frame{} = frame} <- Utils.recv_frame(socket, state.compressor),
             frame = %{frame | atom_keys?: state.atom_keys?},
             %Prepared{} = prepared <- Protocol.decode_response(frame, prepared) do
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

  def handle_execute(_query, payload, options, %__MODULE__{} = state) do
    %{socket: socket, compressor: compressor, atom_keys?: atom_keys?} = state
    assert_valid_compressor(compressor, options[:compressor])

    with :ok <- :gen_tcp.send(socket, payload),
         {:ok, %Frame{} = frame} <- Utils.recv_frame(socket, compressor) do
      {:ok, %{frame | atom_keys?: atom_keys?}, state}
    else
      {:error, reason} ->
        {:disconnect, ConnectionError.new("execute", reason), state}
    end
  end

  def handle_close(query, _options, state) do
    {:ok, query, state}
  end

  def disconnect(_exception, %__MODULE__{socket: socket}) do
    :ok = :gen_tcp.close(socket)
  end

  def ping(%__MODULE__{socket: socket, compressor: compressor} = state) do
    case Utils.request_options(socket, compressor) do
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

  defp startup_connection(socket, supported_options, compressor, options) do
    %{
      "CQL_VERSION" => [cql_version | _],
      "COMPRESSION" => supported_compression_algorithms
    } = supported_options

    requested_options = %{"CQL_VERSION" => cql_version}

    if compressor do
      compression_algorithm = Atom.to_string(compressor.algorithm())

      if compression_algorithm in supported_compression_algorithms do
        requested_options = Map.put(requested_options, "COMPRESSION", compression_algorithm)
        Utils.startup_connection(socket, requested_options, compressor, options)
      else
        {:error,
         ConnectionError.new(
           "startup connection",
           {:unsupported_compression, compressor.algorithm()}
         )}
      end
    else
      Utils.startup_connection(socket, requested_options, compressor, options)
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
              "module (which uses the #{inspect(initial_algorithm)}"
    end
  end
end
