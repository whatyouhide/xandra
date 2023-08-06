defmodule Xandra.Cluster.ControlConnection.ConnectedNode do
  @moduledoc false

  # We used this internally to keep information about a connection to a specific node
  # held by a control connection.

  alias Xandra.Connection.Utils

  @type t() :: %__MODULE__{
          socket: Utils.socket(),
          protocol_module: module(),
          ip: :inet.ip_address(),
          port: :inet.port_number(),

          # This is only nil when initially constructing the struct.
          host: Xandra.Cluster.Host.t() | nil
        }

  defstruct [:socket, :protocol_module, :ip, :port, :host]

  @spec new(Utils.transport(), Utils.socket(), module()) :: {:ok, t()} | {:error, :inet.posix()}
  def new(transport, socket, protocol_module) when is_atom(protocol_module) do
    with {:ok, {ip, port}} <- inet_mod(transport).peername(socket) do
      node = %__MODULE__{
        socket: socket,
        protocol_module: protocol_module,
        ip: ip,
        port: port
      }

      {:ok, node}
    end
  end

  defp inet_mod(:gen_tcp), do: :inet
  defp inet_mod(:ssl), do: :ssl
end
