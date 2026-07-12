defmodule Xandra.Cluster.Token do
  @moduledoc false

  # Functions to compute partition tokens (Murmur3Partitioner) and ScyllaDB
  # shards from routing keys, used for token-aware and shard-aware routing.

  import Bitwise

  alias Xandra.Cluster.Token.Murmur3
  alias Xandra.Prepared

  @min_token -9_223_372_036_854_775_808
  @max_token 9_223_372_036_854_775_807

  @mask64 0xFFFFFFFFFFFFFFFF

  # Computes the Murmur3Partitioner token for the given prepared query and its
  # bound values. Returns :error if the token cannot be computed, for example
  # because the query has no partition key metadata (protocol v3, non-SELECT
  # statements without fully-bound partition keys, and so on) or because values
  # are missing or invalid. This function should never raise: routing is
  # best-effort, and invalid values will surface proper errors when the query
  # itself executes.
  @spec compute(Prepared.t(), Xandra.values()) :: {:ok, integer()} | :error
  def compute(prepared, values)

  def compute(%Prepared{pk_indices: pk_indices} = prepared, values)
      when is_list(pk_indices) and pk_indices != [] do
    case routing_key(prepared, values) do
      {:ok, routing_key} -> {:ok, for_routing_key(routing_key)}
      :error -> :error
    end
  end

  def compute(%Prepared{}, _values) do
    :error
  end

  # Computes the token for an already-serialized routing key.
  @spec for_routing_key(binary()) :: integer()
  def for_routing_key(routing_key) when is_binary(routing_key) do
    case Murmur3.hash(routing_key) do
      # The Murmur3Partitioner reserves the minimum token, and normalizes
      # hashes that land on it to the maximum token.
      @min_token -> @max_token
      token -> token
    end
  end

  # Computes the routing key for a prepared query and its bound values, by
  # serializing the partition key values. For composite partition keys, each
  # component is <<byte_size::16, component::binary, 0>>; for single-component
  # keys, the routing key is the raw serialized value.
  @spec routing_key(Prepared.t(), Xandra.values()) :: {:ok, binary()} | :error
  def routing_key(%Prepared{} = prepared, values) do
    values =
      if is_map(values) do
        Prepared.rewrite_named_params_to_positional(prepared, values)
      else
        values
      end

    bound_columns = List.to_tuple(prepared.bound_columns)
    values = List.to_tuple(values)

    serialized_pk_values =
      for pk_index <- prepared.pk_indices do
        {_keyspace, _table, _name, type} = elem(bound_columns, pk_index)

        case elem(values, pk_index) do
          value when is_nil(value) or value == :not_set -> throw(:cannot_compute_routing_key)
          value -> prepared.protocol_module.encode_value_for_partition_key(type, value)
        end
      end

    case serialized_pk_values do
      [single_component] ->
        {:ok, single_component}

      components ->
        composite =
          for component <- components, into: <<>> do
            # The component size must fit in 16 bits; partition key values
            # bigger than that can't be represented in a routing key.
            if byte_size(component) > 0xFFFF, do: throw(:cannot_compute_routing_key)
            <<byte_size(component)::16, component::binary, 0>>
          end

        {:ok, composite}
    end
  catch
    :throw, :cannot_compute_routing_key -> :error
    # Routing must be best-effort: bad or missing values will raise proper
    # errors when the query is encoded for execution.
    :error, _reason -> :error
  end

  # Computes the ScyllaDB shard that owns the given token, using the
  # "biased-token-round-robin" algorithm (the only one ScyllaDB defines).
  @spec shard(integer(), pos_integer(), non_neg_integer()) :: non_neg_integer()
  def shard(token, nr_shards, sharding_ignore_msb)
      when is_integer(token) and is_integer(nr_shards) and nr_shards > 0 and
             is_integer(sharding_ignore_msb) and sharding_ignore_msb >= 0 do
    biased = band(token + 0x8000000000000000, @mask64)
    biased = band(biased <<< sharding_ignore_msb, @mask64)
    (biased * nr_shards) >>> 64
  end
end
