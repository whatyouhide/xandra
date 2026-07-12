defmodule Xandra.Cluster.TokenRing do
  @moduledoc false

  # The ring of tokens of a cluster, used for token-aware routing. The ring
  # maps each token range to the host that is the *primary replica* for that
  # range: the owner of a token is the host that owns the smallest ring token
  # that is greater than or equal to the token (wrapping around the ring).
  #
  # We represent the ring as a flat tuple of sorted {token, peername} entries so
  # that lookups can binary-search it.

  alias Xandra.Cluster.Host

  @opaque t() :: tuple()

  @spec build([Host.t()]) :: t() | nil
  def build(hosts) when is_list(hosts) do
    entries =
      for %Host{tokens: tokens} = host <- hosts,
          not is_nil(tokens),
          token_string <- tokens,
          token = parse_token(token_string),
          do: {token, Host.to_peername(host)}

    case Enum.sort(entries) do
      [] -> nil
      sorted_entries -> List.to_tuple(sorted_entries)
    end
  catch
    # A token we cannot parse means a partitioner we don't support, so no ring.
    :throw, :unparsable_token -> nil
  end

  # Finds the peername of the host that is the primary replica for the given
  # token.
  @spec owner_peername(t(), integer()) :: {:inet.ip_address() | String.t(), :inet.port_number()}
  def owner_peername(ring, token) when is_tuple(ring) and is_integer(token) do
    index = lower_bound(ring, token, 0, tuple_size(ring))

    # If the token is greater than all ring tokens, it wraps around to the
    # owner of the first ring token.
    index = if index == tuple_size(ring), do: 0, else: index

    {_token, peername} = elem(ring, index)
    peername
  end

  # Binary search for the smallest index whose ring token is >= the given token.
  # Returns tuple_size(ring) if all ring tokens are smaller.
  defp lower_bound(_ring, _token, low, high) when low >= high do
    low
  end

  defp lower_bound(ring, token, low, high) do
    middle = div(low + high, 2)
    {middle_token, _peername} = elem(ring, middle)

    if middle_token >= token do
      lower_bound(ring, token, low, middle)
    else
      lower_bound(ring, token, middle + 1, high)
    end
  end

  # Tokens are stored as strings in the system tables. With the Murmur3
  # partitioner (the only one we support for token-aware routing), they're
  # decimal representations of signed 64-bit integers.
  defp parse_token(token_string) do
    case Integer.parse(token_string) do
      {token, ""} -> token
      _other -> throw(:unparsable_token)
    end
  end
end
