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
end
