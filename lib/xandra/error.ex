defmodule Xandra.Error do
  defexception [:reason, :message]

  def new(reason, message) when is_atom(reason) and is_binary(message) do
    %__MODULE__{reason: reason, message: message}
  end
end
