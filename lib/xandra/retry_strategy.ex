defmodule Xandra.RetryStrategy do
  @moduledoc """
  TODO

  ## Examples

  This is an example of a retry strategy that retries a fixed number of times before failing:

      defmodule MyApp.CounterRetryStrategy do
        @behaviour Xandra.RetryStrategy

        def init(options) do
          Keyword.fetch!(options, :retries_count)
        end

        def handle_retry(_error, _options, _retries_left = 0) do
          :error
        end
      end

  """

  @type strategy :: term

  @callback new(options :: Keyword.t) :: strategy

  @callback retry(error :: term, options :: Keyword.t, strategy) ::
            :error | {:retry, new_options :: Keyword.t, new_strategy :: strategy}
end

defmodule Xandra.RetryStrategy.Fallthrough do
  @moduledoc """
  TODO
  """

  @behaviour Xandra.RetryStrategy

  def new(_options), do: :no_state

  def retry(_error, _options, :no_state), do: :error
end
