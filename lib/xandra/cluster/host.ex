defmodule Xandra.Cluster.Host do
  @moduledoc """
  The data structure to represent a host in a Cassandra cluster.

  The fields of this struct are public. See `t:t/0` for information on their type,
  and [`%Xandra.Cluster.Host{}`](`__struct__/0`) for more information.
  """
  @moduledoc since: "0.15.0"

  @typedoc since: "0.15.0"
  @type t() :: %__MODULE__{
          address: :inet.ip_address() | :inet.hostname(),
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
  @spec to_peername(t()) :: {:inet.ip_address() | :inet.hostname(), :inet.port_number()}
  def to_peername(%__MODULE__{address: address, port: port}) do
    {address, port}
  end

  @doc false
  @doc since: "0.15.0"
  @spec format_peername({:inet.ip_address() | :inet.hostname(), :inet.port_number()}) ::
          String.t()
  def format_peername({address, port}) do
    if ip_address?(address) do
      IO.puts("DEBUG -- format_peername - input #{inspect({address, port})} return #{:inet.ntoa(address)}:#{port}}")
      "#{:inet.ntoa(address)}:#{port}"
    else
      "#{address}:#{port}"
    end
  end

  # TODO: remove the conditional once we depend on OTP 25+.
  if function_exported?(:inet, :is_ip_address, 1) do
    defp ip_address?(term), do: :inet.is_ip_address(term)
  else
    defp ip_address?(term), do: is_tuple(term)
  end
end
