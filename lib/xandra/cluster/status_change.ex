defmodule Xandra.Cluster.StatusChange do
  @moduledoc false

  @enforce_keys [:effect, :address, :port]
  defstruct [:effect, :address, :port]
end
