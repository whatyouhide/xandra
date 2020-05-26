defmodule Xandra.Cluster.TopologyChange do
  @moduledoc false

  defstruct [:effect, :address, :port, :node]
end
