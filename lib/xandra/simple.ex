defmodule Xandra.Simple do
  @moduledoc """
  A data structure used internally to represent simple queries.
  """

  alias Xandra.Frame

  defstruct [
    :statement,
    :values,
    :default_consistency,
    :protocol_module,
    :compressor,
    :custom_payload
  ]

  @typedoc """
  The type for a simple query.

  The fields of this are not meant to be used, but are documented to avoid
  Dialyzer warnings.
  """
  @type t :: %__MODULE__{
          statement: Xandra.statement(),
          values: Xandra.values() | nil,
          default_consistency: atom() | nil,
          protocol_module: module() | nil,
          compressor: module() | nil,
          custom_payload: Xandra.custom_payload() | nil
        }

  @doc false
  @spec encode(t(), Xandra.values(), keyword()) :: iodata()
  def encode(%__MODULE__{} = query, values, options) when is_list(options) do
    Frame.new(:query,
      tracing: options[:tracing],
      stream_id: Keyword.get(options, :stream_id, 0),
      compressor: query.compressor,
      custom_payload: query.custom_payload
    )
    |> query.protocol_module.encode_request(%__MODULE__{query | values: values}, options)
    |> Frame.encode(query.protocol_module)
  end
end
