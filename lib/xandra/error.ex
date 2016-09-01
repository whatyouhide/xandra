defmodule Xandra.Error do
  defstruct [:code, :message]

  def new(code, message) when is_integer(code) and is_binary(message) do
    %__MODULE__{code: code, message: message}
  end
end
