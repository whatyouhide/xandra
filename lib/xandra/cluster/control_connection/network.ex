defmodule Xandra.Cluster.ControlConnection.Network do
  @moduledoc false

  alias Xandra.{Frame, Simple}
  alias Xandra.Connection.Utils
  alias Xandra.Cluster.Host
  alias Xandra.Cluster.ControlConnection.ConnectedNode

  defguardp is_transport(term) when term in [:gen_tcp, :ssl]

  # https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useQuerySystemTableCluster.html
  @select_peers_query """
  SELECT * FROM system.peers
  """

  @select_peers_info_query """
  SELECT peer, data_center, host_id, rack, release_version, schema_version, tokens
  FROM system.peers
  """

  @select_local_query """
  SELECT data_center, host_id, rack, release_version, schema_version, tokens FROM system.local
  """

  @doc """
  Discover the peers in the same data center as the node we're connected to.
  """
  @spec fetch_cluster_topology(Utils.transport(), :inet.port_number(), ConnectedNode.t()) ::
          {:ok, [Host.t()]} | {:error, :closed | :inet.posix()}
  def fetch_cluster_topology(transport, autodiscovered_nodes_port, %ConnectedNode{} = node)
      when is_transport(transport) and is_integer(autodiscovered_nodes_port) do
    with {:ok, [local_node_info]} <- query(transport, node, @select_local_query),
         {:ok, peers} <- query(transport, node, @select_peers_query) do
      local_peer = %Host{
        queried_peer_to_host(local_node_info)
        | address: node.ip,
          port: node.port
      }

      # We filter out the peers with null host_id because they seem to be nodes that are down or
      # decommissioned but not removed from the cluster. See
      # https://github.com/lexhide/xandra/pull/196 and
      # https://user.cassandra.apache.narkive.com/APRtj5hb/system-peers-and-decommissioned-nodes.
      peers =
        for peer_attrs <- peers,
            peer = %Host{queried_peer_to_host(peer_attrs) | port: autodiscovered_nodes_port},
            not is_nil(peer.host_id),
            do: peer

      {:ok, [local_peer | peers]}
    end
  end

  @doc """
  Executes a single query that must return a page, and returns the page as a list of maps.
  """
  @spec query(Utils.transport(), ConnectedNode.t(), String.t()) ::
          {:ok, [map()]} | {:error, :closed | :inet.posix()}
  def query(transport, %ConnectedNode{} = node, statement)
      when is_transport(transport) and is_binary(statement) do
    query = %Simple{statement: statement, values: [], default_consistency: :one}

    payload =
      Frame.new(:query, _options = [])
      |> node.protocol_module.encode_request(query)
      |> Frame.encode(node.protocol_module)

    protocol_format = Xandra.Protocol.frame_protocol_format(node.protocol_module)

    with :ok <- transport.send(node.socket, payload),
         {:ok, %Frame{} = frame} <- recv_frame(transport, node.socket, protocol_format) do
      {%Xandra.Page{} = page, _warnings} = node.protocol_module.decode_response(frame, query)
      {:ok, Enum.to_list(page)}
    end
  end

  @doc """
  Receives a frame on the given `socket`.
  """
  @spec recv_frame(Utils.transport(), Utils.socket(), atom()) ::
          {:ok, Frame.t()} | {:error, term()}
  def recv_frame(transport, socket, protocol_format)
      when is_transport(transport) and is_atom(protocol_format) do
    case Utils.recv_frame(transport, socket, protocol_format, _compressor = nil) do
      {:ok, frame, ""} -> {:ok, frame}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec get_info_about_peer(Utils.transport(), :inet.port_number(), ConnectedNode.t(), term()) ::
          {:ok, Host.t()} | :error
  def get_info_about_peer(transport, autodiscovered_nodes_port, %ConnectedNode{} = node, address) do
    with {:ok, peers} <- query(transport, node, @select_peers_info_query),
         host when not is_nil(host) <- Enum.find(peers, fn peer -> peer["peer"] == address end) do
      {:ok, %Host{queried_peer_to_host(host) | port: autodiscovered_nodes_port}}
    else
      _other -> :error
    end
  end

  @doc """
  Returns `{:error, reason}` if the socket was closes or if there was any data
  coming from the socket. Otherwise, returns `:ok`.
  """
  @spec assert_no_transport_message(Utils.socket()) ::
          :ok | {:error, :data | :tcp_error | :ssl_error | :closed}
  def assert_no_transport_message(socket) do
    receive do
      {kind, ^socket, _data} when kind in [:tcp, :ssl] -> {:error, :data}
      {kind, ^socket, reason} when kind in [:tcp_error, :ssl_error] -> {:error, reason}
      {kind, ^socket} when kind in [:tcp_closed, :ssl_closed] -> {:error, :closed}
    after
      0 -> :ok
    end
  end

  @spec set_socket_active_once(Utils.transport(), Utils.socket()) :: :ok | {:error, :inet.posix()}
  def set_socket_active_once(transport, socket) do
    inet_mod(transport).setopts(socket, active: :once)
  end

  @spec set_socket_passive(Utils.transport(), Utils.socket()) :: :ok | {:error, :inet.posix()}
  def set_socket_passive(transport, socket) do
    inet_mod(transport).setopts(socket, active: false)
  end

  defp queried_peer_to_host(%{"peer" => _} = peer_attrs) do
    {address, peer_attrs} = Map.pop!(peer_attrs, "peer")
    peer_attrs = Map.put(peer_attrs, "address", address)
    queried_peer_to_host(peer_attrs)
  end

  defp queried_peer_to_host(%{} = peer_attrs) do
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

  defp inet_mod(:gen_tcp), do: :inet
  defp inet_mod(:ssl), do: :ssl
end
