defmodule Xandra.Cluster.Host do
  @moduledoc """
  The data structure to represent a host in a Cassandra cluster.

  The fields of this struct are public. See `t:t/0` for information on their type,
  and [`%Xandra.Cluster.Host{}`](`__struct__/0`) for more information.
  """
  @moduledoc since: "0.15.0"

  @typedoc """
  The type for the host struct.

  ## Fields

    * `:address` - the address of the host. It can be either an IP address
      or a hostname. If Xandra managed to *connect* to this host, then the `:address` will
      be the actual IP peer (see `:inet.peername/1`). Otherwise, the `:address` will be
      the parsed IP or the charlist hostname. For example, if you pass `"10.0.2.1"` as
      the address, Xandra will normalize it to `{10, 0, 2, 1}`.

    * `:port` - the port of the host.

    * `:data_center` - the data center of the host, as found in the `system.local` or
      `system.peers` table.

    * `:host_id` - the ID of the host, as found in the `system.local` or
      `system.peers` table.

    * `:rack` - the rack of the host, as found in the `system.local` or
      `system.peers` table.

    * `:release_version` - the release version of the host, as found in the `system.local` or
      `system.peers` table.

    * `:schema_version` - the schema version of the host, as found in the `system.local` or
      `system.peers` table.

    * `:tokens` - the tokens held by the host, as found in the `system.local` or
      `system.peers` table.

  """
  @typedoc since: "0.15.0"
  @type t() :: %__MODULE__{
          address: :inet.ip_address() | String.t(),
          port: :inet.port_number(),
          data_center: String.t(),
          host_id: String.t(),
          rack: String.t(),
          release_version: String.t(),
          schema_version: String.t(),
          tokens: MapSet.t(String.t())
        }

  @doc """
  The struct that represents a host in a Cassandra cluster.

  See `t:t/0` for the type of each field.
  """
  @doc since: "0.15.0"
  defstruct [
    :address,
    :port,
    :data_center,
    :host_id,
    :rack,
    :release_version,
    :schema_version,
    :tokens
  ]

  @doc """
  Formats a host's address and port as a string.

  ## Examples

      iex> host = %Xandra.Cluster.Host{address: {127, 0, 0, 1}, port: 9042}
      iex> Xandra.Cluster.Host.format_address(host)
      "127.0.0.1:9042"

      iex> host = %Xandra.Cluster.Host{address: "example.com", port: 9042}
      iex> Xandra.Cluster.Host.format_address(host)
      "example.com:9042"

  """
  @doc since: "0.15.0"
  @spec format_address(t()) :: String.t()
  def format_address(host) do
    host
    |> to_peername()
    |> format_peername()
  end

  @doc false
  @doc since: "0.15.0"
  @spec to_peername(t()) :: {:inet.ip_address() | String.t(), :inet.port_number()}
  def to_peername(%__MODULE__{address: address, port: port}) do
    {address, port}
  end

  @doc false
  @doc since: "0.15.0"
  @spec format_peername({:inet.ip_address() | String.t(), :inet.port_number()}) ::
          String.t()
  def format_peername({address, port}) when is_integer(port) and port in 0..65_535 do
    if ip_address?(address) do
      "#{:inet.ntoa(address)}:#{port}"
    else
      address <> ":#{port}"
    end
  end

  # TODO: remove the conditional once we depend on OTP 25+.
  if function_exported?(:inet, :is_ip_address, 1) do
    defp ip_address?(term), do: :inet.is_ip_address(term)
  else
    defp ip_address?(term), do: is_tuple(term)
  end
end
