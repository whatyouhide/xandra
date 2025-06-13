defmodule Xandra.ConnectionError do
  @moduledoc """
  An exception struct that represents an error in the connection to the
  Cassandra server.

  For more information on when this error is returned or raised, see the
  documentation for the `Xandra` module.

  The `:action` field represents the action that was being performed when the
  connection error occurred. The `:reason` field represents the reason of the
  connection error: for network errors, this is usually a POSIX reason (like
  `:econnrefused`). The following Xandra-specific reasons are supported:

    * `{:unsupported_compression, algorithm}` - this happens when a
      `:compressor` module has been specified in `Xandra.start_link/1`, but
      negotiating the connection algorithm fails because such compressor module
      uses an algorithm that the Cassandra server does not support.

    * `:disconnected` - the connection closed in the middle of a request to the server.

    * `{:connection_process_crashed, reason}` - the connection process crashed before
      sending a response.

    * `:timeout` - the connection timed out while waiting for a response from the
      server.

    * `{:cluster, :not_connected}` - this happens when a `Xandra.Cluster`-based
      connection is not connected to any node (for example, because all the
      specified nodes are currently down). See the documentation for
      `Xandra.Cluster` for more information.

  Since this struct is an exception, it is possible to raise it with
  `Kernel.raise/1`. If the intent is to format connection errors as strings (for
  example, for logging purposes), it is possible to use `Exception.message/1` to
  get a formatted version of the error.
  """
  defexception [:action, :reason]

  @type t :: %__MODULE__{
          action: String.t(),
          reason: term
        }

  @spec new(String.t(), term) :: t
  def new(action, reason) when is_binary(action) do
    %__MODULE__{action: action, reason: reason}
  end

  @impl true
  def message(%__MODULE__{action: action, reason: reason}) do
    "action \"#{action}\" failed with reason: #{format_reason(reason)}"
  end

  defp format_reason({:unsupported_compression, algorithm}) do
    "unsupported compression algorithm #{inspect(algorithm)}"
  end

  defp format_reason(:closed) do
    "socket is closed"
  end

  defp format_reason(:not_connected) do
    "there is currently no connection established with the server"
  end

  defp format_reason(:disconnected) do
    "connection dropped in the middle of a request"
  end

  defp format_reason(:no_connection_process) do
    "the connection process doesn't exist"
  end

  defp format_reason({:connection_process_crashed, reason}) do
    "connection process crashed before sending a response with reason: #{inspect(reason)}"
  end

  defp format_reason(:timeout) do
    "request timeout"
  end

  defp format_reason(:too_many_concurrent_requests) do
    """
    this connection has too many requests in flight; to fix this, consider:

      1. increasing the size of the connection pool so that you'll have more
         connections available and you'll be able to spread the request load
         over more connections
      2. increasing the maximum number of allowed in-flight requests per connection,
         which can be configured through the :max_concurrent_requests_per_connection
         option when starting connections (defaults to 100)
    """
  end

  defp format_reason({:cluster, :not_connected}) do
    "not connected to any of the nodes"
  end

  defp format_reason({:cluster, :pool_closed}) do
    """
    the Xandra pool is closed, probably because the network connection dropped right after
    Xandra.Cluster.Pool.checkout/0
    """
  end

  defp format_reason(reason) do
    case :inet.format_error(reason) do
      ~c"unknown POSIX error" -> inspect(reason)
      formatted -> List.to_string(formatted)
    end
  end
end
