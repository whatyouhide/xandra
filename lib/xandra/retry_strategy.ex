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

    1. invoke the `c:new/1` callback with options passed to the failing function
       to initialize the given retry strategy

    2. ask the retry strategy whether to retry or error out (`c:retry/3`) until
       either the query succeeds or `c:retry/3` says to error out

  The `c:new/1` and `c:retry/3` callbacks will be invoked in the same
  process that executed the original query.

  There are two levels where RetryStrategy is invoked, distinguishable with the
  `:execution_level` key in the options passed to `c:new/1` and `c:retry/3`,
  namely `:cluster` level and `:xandra` level. On `:cluster` level, you have the option
  to select a `:target_connection` from the list of `:connected_hosts`, in order to
  retry on a different node for instance. The `:connected_hosts` in `options` is a
  list of tuples, where the first element is the Xandra connection pid and the
  second is of `Host.t()` describing the host.

  If on `:cluster` level `c:retry/3` says to retry a query, such query can be retried on the
  Xandra connection that is returned in the new `option` by `c:retry/3` under the `:target_connection`
  key.

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

  A particularly useful application is to retry on queries on different hosts
  when using `Xandra.Cluster`. We can even choose not to execute on certain `Host.t()`s
  (because they may be in a different datacenter). Following example retries on all hosts
  after the first `:connected_node` has failed:

      defmodule AllNodesStrategy do
        @behaviour Xandra.RetryStrategy

        alias Xandra.Cluster.Host

        def new(options) do
          if options[:execution_level] == :cluster do
            [_already_tried_node | rest_of_nodes] = options[:connected_hosts]

            rest_of_nodes
          end
        end

        def retry(_error, options, state) do
          case options[:execution_level] do
            :xandra ->
              :error

            :cluster ->
              case state do
                [] ->
                  :error

                [{conn, %Host{}} | rest_of_nodes] ->
                  options = Keyword.put(options, :target_connection, conn)
                  {:retry, options, rest_of_nodes}
              end
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
  @callback new(options :: keyword) :: state

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

  Note that when `execution_level: :cluster` if we would return a `:target_connection` pid,
  the query would be retried on the specified `Xandra` connection. To select a connection pid,
  we can use `:connected_hosts` key in `options`.

  When retrying on `execution_level: :xandra`, we are retrying with the exact same connection.
  """
  @callback retry(error :: term, options :: keyword, state) ::
              :error | {:retry, new_options :: keyword, new_state :: state}

  @doc false
  @spec run_with_retrying(keyword, (-> result)) :: result
        when result: var
  def run_with_retrying(options, fun) do
    options = Keyword.put(options, :execution_level, :xandra)

    case Keyword.pop(options, :retry_strategy) do
      {nil, _options} ->
        fun.()

      {retry_strategy, options} ->
        run_with_retrying(options, retry_strategy, fun)
    end
  end

  def run_with_retrying(options, retry_strategy, fun) do
    with {:error, reason} <- fun.() do
      {retry_state, options} =
        Keyword.pop_lazy(options, :retrying_state, fn ->
          retry_strategy.new(options)
        end)

      case retry_strategy.retry(reason, options, retry_state) do
        :error ->
          {:error, reason}

        {:retry, new_options, new_retry_state} ->
          new_options = Keyword.put(new_options, :retrying_state, new_retry_state)
          run_with_retrying(new_options, retry_strategy, fun)

        other ->
          raise ArgumentError,
                "invalid return value #{inspect(other)} from " <>
                  "retry strategy #{inspect(retry_strategy)} " <>
                  "with state #{inspect(retry_state)}"
      end
    end
  end

  @spec run_cluster_with_retrying(Keyword.t(), [{pid(), Host.t()}, ...], (pid() -> result)) ::
          result
        when result: var
  def run_cluster_with_retrying(options, connected_hosts, fun) do
    [{conn, _host} | _connected_hosts] = connected_hosts

    options =
      Keyword.merge(options,
        execution_level: :cluster,
        connected_hosts: connected_hosts,
        target_connection: conn
      )

    case Keyword.pop(options, :retry_strategy) do
      {nil, _options} ->
        fun.(conn)

      {retry_strategy, options} ->
        run_cluster_with_retrying(options, connected_hosts, retry_strategy, fun)
    end
  end

  defp run_cluster_with_retrying(options, connected_hosts, retry_strategy, fun) do
    {conn, options} =
      case Keyword.pop(options, :target_connection) do
        {conn, options} when is_pid(conn) ->
          {conn, options}

        {:random, options} ->
          [{conn, _host}] = Enum.take_random(connected_hosts, 1)
          {conn, options}
      end

    with {:error, reason} <- fun.(conn) do
      {retry_state, options} =
        Keyword.pop_lazy(options, :retrying_state, fn ->
          retry_strategy.new(options)
        end)

      case retry_strategy.retry(reason, options, retry_state) do
        :error ->
          {:error, reason}

        {:retry, new_options, new_retry_state} ->
          new_options =
            Keyword.put(new_options, :retrying_state, new_retry_state)
            |> Keyword.put_new(:target_connection, :random)

          run_cluster_with_retrying(new_options, connected_hosts, retry_strategy, fun)

        other ->
          raise ArgumentError,
                "invalid return value #{inspect(other)} from " <>
                  "retry strategy #{inspect(retry_strategy)} " <>
                  "with state #{inspect(retry_state)}"
      end
    end
  end
end
