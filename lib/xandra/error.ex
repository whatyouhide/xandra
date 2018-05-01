defmodule Xandra.Error do
  @moduledoc """
  An exception struct that represents an error returned by Cassandra.

  For more information on when this error is returned or raised, see the
  documentation for the `Xandra` module.

  The `:reason` field represents the reason (as an atom) of the error. For
  example, if the query you're trying to execute contains a syntax error,
  `:reason` will be `:invalid_syntax`. The `:message` field is a string that
  contains the exact error message that Cassandra returned.

  Since this struct is an exception, it is possible to raise it with
  `Kernel.raise/1`. If the intent is to format errors as strings (for
  example, for logging purposes), it is possible to use `Exception.message/1` to
  get a formatted version of the error.
  """
  defexception [:reason, :message]

  @type t :: %__MODULE__{
          reason: atom,
          message: String.t()
        }

  @spec new(atom, String.t()) :: t
  def new(reason, message) when is_atom(reason) and is_binary(message) do
    %__MODULE__{reason: reason, message: message}
  end
end
