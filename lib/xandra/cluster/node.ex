defmodule Xandra.Cluster.Node do
  @enforce_keys [
    :address,
    :data_center,
    :rack
  ]

  defstruct @enforce_keys

  def new(node_info) do
    %{
      "rpc_address" => address,
      "rack" => rack,
      "data_center" => data_center
    } = node_info

    %__MODULE__{
      address: address,
      rack: rack,
      data_center: data_center
    }
  end
end
