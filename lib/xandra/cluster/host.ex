defmodule Xandra.Cluster.Host do
  @moduledoc """
  TODO
  """
  @moduledoc since: "0.15.0"

  @type t() :: %__MODULE__{
          address: :inet.ip_address(),
          port: :inet.port_number(),
          data_center: String.t(),
          host_id: String.t(),
          rack: String.t(),
          release_version: String.t(),
          schema_version: String.t(),
          tokens: MapSet.t(String.t())
        }

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
