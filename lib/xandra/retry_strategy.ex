defmodule Xandra.RetryStrategy do
  @moduledoc """
  A behaviour that handles how to retry failed queries.

  This behaviour makes it possible to customize the strategy that Xandra uses to
  retry failed queries. By default, Xandra does not retry failed queries, and
  does not provide any default retry strategy since retrying queries based on
  the failure reason is very tied to application logic.

  A module that implements the `Xandra.RetryStrategy` behaviour can be passed to
  several functions in the `Xandra` module: look at the documentation for
  `Xandra` for more information.

  ## Usage

  When a query fails and a retry strategy module was passed as an option, Xandra
  will:

    1. invoke the `c:new/1` callback with the options passed to the failing
       function to initialize the given retry strategy

    1. ask the retry strategy whether to retry or error out (`c:retry/3`) until
       either the query succeeds or `c:retry/3` says to error out

  The `c:new/1` and `c:retry/3` callbacks will be invoked in the same
  process that executed the original query.

  If `c:retry/3` says to retry a query, such query will be retried on a
  different Xandra connection than the one the query was first executed
  through. For more information, see the documentation for `c:retry/3`.

  ## Examples

  This is an example of a retry strategy that retries a fixed number of times
  before failing. It reads the allowed number of retries from the options.

      defmodule MyApp.CounterRetryStrategy do
        @behaviour Xandra.RetryStrategy

        def new(options) do
          Keyword.fetch!(options, :retry_count)
        end

        def retry(_error, _options, _retries_left = 0) do
          :error
        end

        def retry(_error, options, retries_left) do
          {:retry, options, retries_left - 1}
        end
      end

  Another interesting example could be a retry strategy based on downgrading
  consistency: for example, we could execute all queries with a high consistency
  (such as `:all`) at first, and in case of failure, try again with a lower
  consistency (such as `:quorum`), finally giving up if that fails as well.

      defmodule MyApp.DowngradingConsistencyRetryStrategy do
        @behaviour Xandra.RetryStrategy

        def new(_options) do
          :no_state
        end

        def retry(_error, options, :no_state) do
          case Keyword.fetch(options, :consistency) do
            # No consistency was specified, so we don't bother to retry.
            :error ->
              :error
            {:ok, :all} ->
              {:retry, Keyword.put(options, :consistency, :quorum), :no_state}
            {:ok, _other} ->
              :error
          end
        end
      end

  """

  @type state :: term

  @doc """
  Initializes the state of a retry strategy based on the given `options`.
  """
  @callback new(options :: Keyword.t()) :: state

  @doc """
  Determines whether to retry the failed query or return the error.

  The first argument is the error that caused the query to fail: for example, it
  could be a `Xandra.Error` struct with reason `:read_timeout`. This can be used
  to determine the retry strategy based on the failure reason. The second
  argument is the options given to the function that failed while executing the
  query. The third argument is the retry strategy state returned either by
  `c:new/1` (if this was the first attempt to retry) or by subsequent calls to
  `c:retry/3`.

  If `:error` is returned, the function that was trying to execute the query
  will return the error to the caller instead of retrying.

  If `{:retry, new_options, new_state}` is returned, the function that was
  trying to execute the query will be invoked again with the same query and
  `new_options` as its options. `new_state` will be used if the query fails
  again: in that case, `c:retry/3` will be invoked again with `new_state` as its
  third argument. This process will continue until either the query is executed
  successfully or this callback returns `:error`.

  Note that when `{:retry, new_options, new_state}` is returned, the query will
  be executed again *on a different Xandra connection*. This behaviour is
  particularly useful with pooled connections and especially when using
  `Xandra.Cluster` as the pool, since it will mean that there's a chance the
  retried query will be executed on a different node altogether.
  """
  @callback retry(error :: term, options :: Keyword.t(), state) ::
              :error | {:retry, new_options :: Keyword.t(), new_state :: state}
end
