defmodule Xandra.Error do
  defexception [:reason, :message]

  @type t :: %__MODULE__{
    reason: atom,
    message: String.t,
  }

  @spec new(atom, String.t) :: t
  def new(reason, message) when is_atom(reason) and is_binary(message) do
    %__MODULE__{reason: reason, message: message}
  end
end
