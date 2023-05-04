defmodule Xandra.Cluster.StatusChange do
  @moduledoc false

  @type t() :: %__MODULE__{}

  @enforce_keys [:effect, :address, :port]
  defstruct [:effect, :address, :port]
end
