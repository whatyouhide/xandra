defmodule Xandra.Connection.Error do
  defexception [:action, :reason]

  @type t :: %__MODULE__{
    action: String.t,
    reason: atom,
  }

  @spec new(String.t, atom) :: t
  def new(action, reason) when is_binary(action) and is_atom(reason) do
    %__MODULE__{action: action, reason: reason}
  end

  def message(%__MODULE__{} = exception) do
    "on action \"#{exception.action}\": #{format_reason(exception.reason)}"
  end

  defp format_reason(reason) do
    case :inet.format_error(reason) do
      'unknown POSIX error' -> inspect(reason)
      formatted -> List.to_string(formatted)
    end
  end
end
