defmodule Xandra.Cluster.ControlConnection do
  @moduledoc false

  # A control connection is a simple GenServer that connects to a given node,
  # and periodically refreshes the cluster topology. It also listens to status/topology
  # change events emitted by the sever. It sends all the info it discovers to the
  # parent cluster as Erlang messages.
  # If this process can't connect to the given node, it stops. If the connection breaks,
  # it stops. All the logic about reconnecting to different hosts and such lives in the
  # cluster itself, not here.

  use GenServer

  alias Xandra.{Frame, Connection.Utils, Simple, Transport}
  alias Xandra.Cluster.{Host, StatusChange, TopologyChange}

  require Logger

  @default_connect_timeout 5_000
  @forced_transport_options [packet: :raw, mode: :binary, active: false]
  @delay_after_topology_change 5_000

  # Internal NimbleOptions schema used to validate the options given to start_link/1.
  # This is only used for internal consistency and having an additional layer of
  # weak "type checking" (some people might get angry at this).
  @opts_schema [
    cluster_pid: [type: :pid, required: true],
    cluster_name: [type: :any, required: true],
    contact_node: [type: {:tuple, [{:or, [:string, :any]}, :non_neg_integer]}, required: true],
    connection_options: [type: :keyword_list, required: true],
    autodiscovered_nodes_port: [type: :non_neg_integer, required: true],
    refresh_topology_interval: [type: :timeout, required: true],
    use_rpc_address_for_peer_address: [type: :boolean, default: false, required: false]
  ]

  defstruct [
    # The PID and name of the parent cluster.
    :cluster_pid,
    :cluster_name,

    # The node to contact to get the cluster topology.
    :contact_node,

    # The transport and its options.
    :transport,

    # The options to use to connect to the node.
    :connection_options,

    # The same as in the cluster.
    :autodiscovered_nodes_port,

    # The interval at which to refresh the cluster topology.
    :refresh_topology_interval,

    # In the system.peers table use the `rpc_address` column for the
    # peer/Host address and not the `peer` column
    :use_rpc_address_for_peer_address,

    # The protocol module of the node we're connected to.
    :protocol_module,

    # The IP/port of the node we're connected to.
    :ip,
    :port,

    # Data buffer.
    buffer: <<>>
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) when is_list(options) do
    options = NimbleOptions.validate!(options, @opts_schema)

    connection_opts = Keyword.fetch!(options, :connection_options)
    {transport, connection_opts} = transport_from_connection_opts(connection_opts)

    %Host{} =
      contact_node =
      case Keyword.fetch!(options, :contact_node) do
        {host, port} when is_list(host) -> %Host{address: host, port: port}
        {host, port} when is_tuple(host) -> %Host{address: host, port: port}
      end

    state = %__MODULE__{
      cluster_pid: Keyword.fetch!(options, :cluster_pid),
      cluster_name: Keyword.get(options, :cluster_name),
      contact_node: contact_node,
      autodiscovered_nodes_port: Keyword.fetch!(options, :autodiscovered_nodes_port),
      refresh_topology_interval: Keyword.fetch!(options, :refresh_topology_interval),
      use_rpc_address_for_peer_address: Keyword.fetch!(options, :use_rpc_address_for_peer_address),
      connection_options: connection_opts,
      transport: transport
    }

    GenServer.start_link(__MODULE__, state, [])
  end

  defp transport_from_connection_opts(connection_opts) do
    module = if connection_opts[:encryption], do: :ssl, else: :gen_tcp

    {transport_opts, connection_opts} = Keyword.pop(connection_opts, :transport_options, [])

    transport = %Transport{
      module: module,
      options: Keyword.merge(transport_opts, @forced_transport_options)
    }

    {transport, connection_opts}
  end

  ## Callbacks

  # Connect right as we start, in a blocking fashion.
  @impl true
  def init(%__MODULE__{} = state) do
    case connect(state) do
      {:ok, state, peers} ->
        state = refresh_topology(state, peers)

        execute_telemetry(state, [:control_connection, :connected], %{}, %{})

        # We set up a timer to periodically refresh the topology.
        schedule_refresh_topology(state.refresh_topology_interval)

        {:ok, state}

      {:error, reason} ->
        execute_telemetry(state, [:control_connection, :failed_to_connect], %{}, %{reason: reason})

        {:stop, {:shutdown, reason}}
    end
  end

  @impl true
  def handle_continue({:disconnected, reason}, %__MODULE__{} = state) do
    execute_telemetry(state, [:control_connection, :disconnected], %{}, %{reason: reason})
    {:stop, {:shutdown, reason}, state}
  end

  @impl true
  def handle_info(message, state)

  def handle_info(:refresh_topology, %__MODULE__{} = state) do
    with :ok <- Transport.setopts(state.transport, active: false),
         :ok <- assert_no_transport_message(state.transport.socket),
         {:ok, peers} <-
           fetch_cluster_topology(
             state.transport,
             state.autodiscovered_nodes_port,
             state.protocol_module,
             state.ip,
             state.port,
             state.use_rpc_address_for_peer_address
           ),
         :ok <- Transport.setopts(state.transport, active: :once) do
      state = refresh_topology(state, peers)
      schedule_refresh_topology(state.refresh_topology_interval)
      {:noreply, state}
    else
      {:error, reason} ->
        state = update_in(state.transport, &Transport.close/1)
        {:noreply, state, {:continue, {:disconnected, reason}}}
    end
  end

  def handle_info({kind, socket, reason}, %__MODULE__{transport: %{socket: socket}} = state)
      when kind in [:tcp_error, :ssl_error] do
    {:noreply, state, {:continue, {:disconnected, reason}}}
  end

  def handle_info({kind, socket}, %__MODULE__{transport: %{socket: socket}} = state)
      when kind in [:tcp_closed, :ssl_closed] do
    {:noreply, state, {:continue, {:disconnected, :closed}}}
  end

  # New data.
  def handle_info({kind, socket, bytes}, %__MODULE__{transport: %{socket: socket}} = state)
      when kind in [:tcp, :ssl] do
    :ok = Transport.setopts(state.transport, active: :once)

    state =
      state
      |> update_in([Access.key(:buffer)], &(&1 <> bytes))
      |> consume_new_data()

    {:noreply, state}
  end

  @impl true
  def handle_cast({:refresh_topology, peers}, %__MODULE__{} = state) do
    {:noreply, refresh_topology(state, peers)}
  end

  ## Helper functions

  defp connect(%__MODULE__{contact_node: %Host{} = host} = state) do
    %__MODULE__{connection_options: options, transport: transport} = state

    # A nil :protocol_version means "negotiate". A non-nil one means "enforce".
    proto_vsn = Keyword.get(options, :protocol_version)

    case Transport.connect(
           transport,
           host.address,
           host.port,
           @default_connect_timeout
         ) do
      {:ok, transport} ->
        state = %__MODULE__{state | transport: transport}

        with {:ok, supported_opts, proto_mod} <- Utils.request_options(transport, proto_vsn),
             state = %__MODULE__{state | protocol_module: proto_mod},
             :ok <- startup_connection(state, supported_opts),
             {:ok, {local_address, local_port}} <- Transport.address_and_port(transport),
             state = %__MODULE__{state | ip: local_address, port: local_port},
             {:ok, peers} <-
               fetch_cluster_topology(
                 transport,
                 state.autodiscovered_nodes_port,
                 state.protocol_module,
                 state.ip,
                 state.port,
                 state.use_rpc_address_for_peer_address
               ),
             :ok <- register_to_events(state),
             :ok <- Transport.setopts(state.transport, active: :once) do
          [local_host | _] = peers
          state = %__MODULE__{state | ip: local_host.address, port: local_host.port}
          {:ok, state, peers}
        else
          {:error, {:use_this_protocol_instead, _failed_protocol_version, proto_vsn}} ->
            state = update_in(state.transport, &Transport.close/1)

            state =
              update_in(state.connection_options, &Keyword.put(&1, :protocol_version, proto_vsn))

            connect(state)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp refresh_topology(%__MODULE__{} = state, new_peers) do
    :telemetry.execute([:xandra, :cluster, :discovered_peers], %{peers: new_peers}, %{})
    send(state.cluster_pid, {:discovered_hosts, new_peers})
    state
  end

  defp startup_connection(%__MODULE__{} = state, supported_options) do
    %{"CQL_VERSION" => [cql_version | _]} = supported_options

    Utils.startup_connection(
      state.transport,
      _requested_options = %{"CQL_VERSION" => cql_version},
      state.protocol_module,
      _compressor = nil,
      state.connection_options
    )
  end

  defp register_to_events(%__MODULE__{protocol_module: protocol_module} = state) do
    payload =
      Frame.new(:register, _options = [])
      |> protocol_module.encode_request(["STATUS_CHANGE", "TOPOLOGY_CHANGE"])
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- Transport.send(state.transport, payload),
         {:ok, %Frame{} = frame} <- recv_frame(state.transport, protocol_format) do
      :ok = state.protocol_module.decode_response(frame)
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_change_event(state, %StatusChange{effect: "UP", address: address, port: port}) do
    send(state.cluster_pid, {:host_up, address, port})
    state
  end

  defp handle_change_event(state, %StatusChange{effect: "DOWN", address: address, port: port}) do
    send(state.cluster_pid, {:host_down, address, port})
    state
  end

  # When we get a NEW_NODE, we need to re-query the system.peers to get info about the new node.
  # When we get REMOVED_NODE, we can still re-query system.peers to check.
  # Might as well just refresh the topology, right?
  defp handle_change_event(state, %TopologyChange{effect: effect})
       when effect in ["NEW_NODE", "REMOVED_NODE"] do
    schedule_refresh_topology(@delay_after_topology_change)
    state
  end

  defp handle_change_event(state, %TopologyChange{effect: "MOVED_NODE"} = event) do
    Logger.warning("Ignored TOPOLOGY_CHANGE event: #{inspect(event)}")
    state
  end

  defp schedule_refresh_topology(timeout) do
    Process.send_after(self(), :refresh_topology, timeout)
  end

  defp consume_new_data(%__MODULE__{} = state) do
    fetch_bytes_fun = fn binary, byte_count ->
      case binary do
        <<part::binary-size(byte_count), rest::binary>> -> {:ok, part, rest}
        _other -> {:error, :not_enough_data}
      end
    end

    rest_fun = & &1

    function =
      case state.protocol_module do
        Xandra.Protocol.V5 -> :decode_v5
        Xandra.Protocol.V4 -> :decode_v4
        Xandra.Protocol.V3 -> :decode_v4
      end

    case apply(Xandra.Frame, function, [
           fetch_bytes_fun,
           state.buffer,
           _compressor = nil,
           rest_fun
         ]) do
      {:ok, frame, rest} ->
        change_event = state.protocol_module.decode_response(frame)
        state = handle_change_event(state, change_event)
        consume_new_data(%__MODULE__{state | buffer: rest})

      {:error, _reason} ->
        state
    end
  end

  # https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useQuerySystemTableCluster.html
  @select_peers_query """
  SELECT * FROM system.peers
  """

  @select_local_query """
  SELECT data_center, host_id, rack, release_version, schema_version, tokens FROM system.local
  """

  @doc """
  Discover the peers in the same data center as the node we're connected to.
  """
  @spec fetch_cluster_topology(
          Transport.t(),
          :inet.port_number(),
          module(),
          :inet.ip_address(),
          :inet.port_number(),
          boolean()
        ) ::
          {:ok, [Host.t()]} | {:error, :closed | :inet.posix()}
  def fetch_cluster_topology(
        %Transport{} = transport,
        autodiscovered_nodes_port,
        protocol_module,
        ip,
        port,
        use_rpc_address
      )
      when is_integer(autodiscovered_nodes_port) and is_atom(protocol_module) do
    with {:ok, [local_node_info]} <- query(transport, protocol_module, @select_local_query),
         {:ok, peers} <- query(transport, protocol_module, @select_peers_query) do
      local_peer = %Host{
        queried_peer_to_host(local_node_info, use_rpc_address)
        | address: ip,
          port: port
      }

      # We filter out the peers with null host_id because they seem to be nodes that are down or
      # decommissioned but not removed from the cluster. See
      # https://github.com/lexhide/xandra/pull/196 and
      # https://user.cassandra.apache.narkive.com/APRtj5hb/system-peers-and-decommissioned-nodes.
      peers =
        for peer_attrs <- peers,
            peer = %Host{queried_peer_to_host(peer_attrs, use_rpc_address) | port: autodiscovered_nodes_port},
            not is_nil(peer.host_id),
            do: peer

      {:ok, [local_peer | peers]}
    end
  end

  defp query(%Transport{} = transport, protocol_module, statement)
       when is_atom(protocol_module) and is_binary(statement) do
    query = %Simple{statement: statement, values: [], default_consistency: :one}

    payload =
      Frame.new(:query, _options = [])
      |> protocol_module.encode_request(query)
      |> Frame.encode(protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(protocol_module)

    with :ok <- Transport.send(transport, payload),
         {:ok, %Frame{} = frame} <- recv_frame(transport, protocol_format) do
      {%Xandra.Page{} = page, _warnings} = protocol_module.decode_response(frame, query)
      {:ok, Enum.to_list(page)}
    end
  end

  defp assert_no_transport_message(socket) do
    receive do
      {kind, ^socket, _data} when kind in [:tcp, :ssl] -> {:error, :data}
      {kind, ^socket, reason} when kind in [:tcp_error, :ssl_error] -> {:error, reason}
      {kind, ^socket} when kind in [:tcp_closed, :ssl_closed] -> {:error, :closed}
    after
      0 -> :ok
    end
  end

  defp queried_peer_to_host(%{"rpc_address" => rpc_address} = peer_attrs, use_rpc_address) when is_tuple(rpc_address) do
    {address, peer_attrs} = Map.pop!(peer_attrs, "rpc_address")
    peer_attrs = Map.delete(peer_attrs, "peer")
    peer_attrs = Map.put(peer_attrs, "address", address)
    queried_peer_to_host(peer_attrs, use_rpc_address)
  end

  defp queried_peer_to_host(%{"rpc_address" => _} = peer_attrs, use_rpc_address) do
    {address, peer_attrs} = Map.pop!(peer_attrs, "rpc_address")
    peer_attrs = Map.delete(peer_attrs, "peer")
    peer_attrs =
    case :inet.parse_address(address) do
      {:ok, valid_address_tuple} ->
        Map.put(peer_attrs, "address", valid_address_tuple)

      error ->
        Logger.error("queried_peer_to_host: error converting address (#{inspect(address)}) to tuple, error: #{inspect(error)}")
        # failed to parse, however, use what was returned in the table, see if
        # node_validation will pass on it
        Map.put(peer_attrs, "address", address)
    end
    queried_peer_to_host(peer_attrs, use_rpc_address)
  end


  # defp queried_peer_to_host(%{"rpc_address" => rpc_address} = peer_attrs, true = use_rpc_address) when is_tuple(rpc_address) do
  #   {address, peer_attrs} = Map.pop!(peer_attrs, "rpc_address")
  #   peer_attrs = Map.delete(peer_attrs, "peer")
  #   peer_attrs = Map.put(peer_attrs, "address", address)
  #   queried_peer_to_host(peer_attrs, use_rpc_address)
  # end

  # defp queried_peer_to_host(%{"rpc_address" => _} = peer_attrs, true = use_rpc_address) do
  #   {address, peer_attrs} = Map.pop!(peer_attrs, "rpc_address")
  #   peer_attrs = Map.delete(peer_attrs, "peer")
  #   peer_attrs =
  #   case :inet.parse_address(address) do
  #     {:ok, valid_address_tuple} ->
  #       Map.put(peer_attrs, "address", valid_address_tuple)

  #     error ->
  #       Logger.error("queried_peer_to_host: error converting address (#{inspect(address)}) to tuple, error: #{inspect(error)}")
  #       # failed to parse, however, use what was returned in the table, see if
  #       # node_validation will pass on it
  #       Map.put(peer_attrs, "address", address)
  #   end
  #   queried_peer_to_host(peer_attrs, use_rpc_address)
  # end

  # defp queried_peer_to_host(%{"peer" => peer} = peer_attrs, use_rpc_address) when is_tuple(peer) do
  #   {address, peer_attrs} = Map.pop!(peer_attrs, "peer")
  #   peer_attrs = Map.delete(peer_attrs, "rpc_address")
  #   peer_attrs = Map.put(peer_attrs, "address", address)
  #   queried_peer_to_host(peer_attrs, use_rpc_address)
  # end

  # defp queried_peer_to_host(%{"peer" => _} = peer_attrs, use_rpc_address) do
  #   {address, peer_attrs} = Map.pop!(peer_attrs, "peer")
  #   peer_attrs = Map.delete(peer_attrs, "rpc_address")
  #   peer_attrs =
  #   case :inet.parse_address(address) do
  #     {:ok, valid_address_tuple} ->
  #       Map.put(peer_attrs, "address", valid_address_tuple)

  #     error ->
  #       Logger.error("queried_peer_to_host: error converting address (#{inspect(address)}) to tuple, error: #{inspect(error)}")
  #       # failed to parse, however, use what was returned in the table, see if
  #       # node_validation will pass on it
  #       Map.put(peer_attrs, "address", address)
  #   end

  #   queried_peer_to_host(peer_attrs, use_rpc_address)
  # end

  defp queried_peer_to_host(%{} = peer_attrs, _) do
    columns = [
      "address",
      "data_center",
      "host_id",
      "rack",
      "release_version",
      "schema_version",
      "tokens"
    ]

    peer_attrs =
      peer_attrs
      |> Map.take(columns)
      |> Enum.map(fn {key, val} -> {String.to_existing_atom(key), val} end)

    struct!(Host, peer_attrs)
  end

  defp recv_frame(%Transport{} = transport, protocol_format) when is_atom(protocol_format) do
    case Utils.recv_frame(transport, protocol_format, _compressor = nil) do
      {:ok, frame, ""} -> {:ok, frame}
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute_telemetry(%__MODULE__{} = state, event_postfix, measurements, extra_meta) do
    base_meta = %{
      cluster_name: state.cluster_name,
      cluster_pid: state.cluster_pid,
      host: if(state.ip, do: %Host{address: state.ip, port: state.port}, else: state.contact_node)
    }

    :telemetry.execute(
      [:xandra, :cluster] ++ event_postfix,
      measurements,
      Map.merge(base_meta, extra_meta)
    )
  end
end
