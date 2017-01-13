defmodule Xandra.SchemaChange do
  defstruct [:effect, :target, :options]

  @type t :: %__MODULE__{
    effect: String.t,
    target: String.t,
    options: map,
  }
end
