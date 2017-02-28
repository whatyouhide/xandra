defmodule Xandra.RetryStrategy do
  @moduledoc """
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

  @callback init(options :: Keyword.t) :: strategy

  @callback handle_retry(error :: term, options :: Keyword.t, strategy) ::
            :ignore | :error | {:retry, new_options :: Keyword.t, new_strategy :: strategy}
end
