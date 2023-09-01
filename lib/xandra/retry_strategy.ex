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

    1. invoke the `c:new/2` callback with the currently connected hosts and options
       passed to the failing function to initialize the given retry strategy

    2. ask the retry strategy whether to retry or error out (`c:retry/3`) until
       either the query succeeds or `c:retry/3` says to error out

  The `c:new/2` and `c:retry/3` callbacks will be invoked in the same
  process that executed the original query.

  The first argument of `c:new/2` is a list of tuples, where the first element is the
  Xandra connection pid and the second is of `Host.t()` describing the host.

  If `c:retry/3` says to retry a query, such query will be retried on the
  Xandra connection that is returned by `c:retry/3`. You will need to specify which
  host is to be retried on. For more information, see the documentation for `c:retry/3`.

  ## Examples

  This is an example of a retry strategy that retries a fixed number of times on
  different hosts that are connected before failing. It reads the allowed number
  of retries from the options.

      defmodule MyApp.CounterRetryStrategy do
        @behaviour Xandra.RetryStrategy

        def new(connected_hosts, options) do
          retries_left = Keyword.fetch!(options, :retry_count)

          [_already_tried_host | remaining_hosts] = connected_hosts

          %{remaining_hosts: remaining_hosts, retries_left: retries_left}
        end

        def retry(_error, _options, %{retries_left: 0}) do
          :error
        end

        def retry(_error, _options, %{remaining_hosts: []}) do
          :error
        end

        def retry(_error, options, %{remaining_hosts: remaining_hosts, retries_left: retries_left}) do
          [{conn, _host} | remaining_hosts] = remaining_hosts

          {:retry, conn, options, %{remaining_hosts: remaining_hosts, retries_left: retries_left - 1}}
        end
      end

  Another interesting example could be a retry strategy based on downgrading
  consistency: for example, we could execute all queries with a high consistency
  (such as `:all`) at first, and in case of failure, try again with a lower
  consistency (such as `:quorum`), finally giving up if that fails as well.

      defmodule MyApp.DowngradingConsistencyRetryStrategy do
        @behaviour Xandra.RetryStrategy

        def new(connected_hosts, options) do
          [{conn, _host} | _rest_of_hosts] = connected_hosts
          conn
        end

        def retry(_error, options, conn) do
          case Keyword.fetch(options, :consistency) do
            # No consistency was specified, so we don't bother to retry.
            :error ->
              :error
            {:ok, :all} ->
              {:retry, conn, Keyword.put(options, :consistency, :quorum), conn}
            {:ok, _other} ->
              :error
          end
        end
      end

  """

  alias Xandra.Cluster.Host

  @type state :: term

  @doc """
  Initializes the state of a retry strategy based on the given `options`.

  `connected_hosts` is a list of tuples with Xandra connection pids as its first
  element and the `Host.t()` information as second. You would need to save the connection
  information to the state as applicable to your retry logic in order to select the next
  host in `c:retry/3`. See ##Examples about an example.
  """
  @callback new(connected_hosts :: [{pid(), Host.t()}], options :: keyword) :: state

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

  If `{:retry, conn, new_options, new_state}` is returned, the function that was
  trying to execute the query will be invoked again with the same query and
  `new_options` as its options. `new_state` will be used if the query fails
  again: in that case, `c:retry/3` will be invoked again with `new_state` as its
  third argument. This process will continue until either the query is executed
  successfully or this callback returns `:error`.

  Note that when `{:retry, conn, new_options, new_state}` is returned, the query will
  be executed again on the returned `conn`. This behaviour is particularly useful with
  pooled connections, where you can specify on which Xandra connection the query should be
  retried on by examining the `Host.t()` information to select which node to try next,
  or on which nodes not to retry at all. Note that initially, you will need to return the
  connections of interest in `c:new/2` as part of `state` in order to be able to have
  access to it in this callback.
  """
  @callback retry(error :: term, options :: keyword, state) ::
              :error | {:retry, connection :: pid(), new_options :: keyword, new_state :: state}

  @doc false
  @spec run_with_retrying(keyword, nonempty_list({pid(), Host.t()}), (-> result)) :: result
        when result: var
  def run_with_retrying(options, connected_hosts, fun) do
    {conn, _host} = List.first(connected_hosts)

    case Keyword.pop(options, :retry_strategy) do
      {nil, _options} ->
        fun.(conn)

      {retry_strategy, options} ->
        run_with_retrying(conn, connected_hosts, options, retry_strategy, fun)
    end
  end

  defp run_with_retrying(conn, connected_hosts, options, retry_strategy, fun) do
    with {:error, reason} <- fun.(conn) do
      {retry_state, options} =
        Keyword.pop_lazy(options, :retrying_state, fn ->
          retry_strategy.new(connected_hosts, options)
        end)

      case retry_strategy.retry(reason, options, retry_state) do
        :error ->
          {:error, reason}

        {:retry, conn, new_options, new_retry_state} ->
          new_options = Keyword.put(new_options, :retrying_state, new_retry_state)
          run_with_retrying(conn, connected_hosts, new_options, retry_strategy, fun)

        other ->
          raise ArgumentError,
                "invalid return value #{inspect(other)} from " <>
                  "retry strategy #{inspect(retry_strategy)} " <>
                  "with state #{inspect(retry_state)}"
      end
    end
  end
end
