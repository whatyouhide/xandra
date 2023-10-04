defmodule Xandra.RetryStrategy do
  @moduledoc """
  A behaviour that handles how to retry failed queries.

  This behaviour makes it possible to customize the strategy that Xandra uses to
  *retry failed queries*. By default, Xandra does not retry failed queries, and
  does not provide any default retry strategy since retrying queries based on
  the failure reason is very tied to application logic.

  > #### Why Do You Need This? {: .info}
  >
  > You might be wondering why there's a need for a retry strategy behaviour, when
  > you could do this at the application layer by simply checking the return value
  > of Xandra calls and potentially retrying.
  >
  > Well, the reason is that retrying queries to Cassandra can get quite smart. For
  > example, you might want context to know what Cassandra node a query failed to
  > execute on, so that you can try it *on a different node* next. `Xandra.RetryStrategy`
  > modules get all the necessary info to implement this kind of smarter strategies.

  A module that implements the `Xandra.RetryStrategy` behaviour can be passed to
  several functions in the `Xandra` module and `Xandra.Cluster` modules. look at
  the documentation for those modules for more information.

  ## Usage

  When a query fails and a retry strategy module was passed as an option, Xandra
  will:

    1. **Invoke the `c:new/1` callback** — it will invoke this with the options
       passed to the failing function to initialize the given retry strategy.
       This gives you access to things like default consistency, timeouts, and
       so on.

    1. **Invoke the `c:retry/3` callback until necessary** — Xandra will ask the
       retry strategy whether to retry or error out until either
       the query succeeds or `c:retry/3` says to error out.

  > #### Process {: .neutral}
  >
  > The `c:new/1` and `c:retry/3` callbacks will be invoked in the same
  > process that executed the original query.

  ### Single Connections or Clusters

  There are two possible cases where a retry strategy is invoked: either it's invoked
  when a query fails to execute on a single connection (that is, it was executed through
  a `Xandra` function), or when a query fails to execute through a cluster connection (that
  is, it was executed through a `Xandra.Cluster` function).

  To distinguish these cases, Xandra always passes the `:execution_level` option
  to `c:new/1`. This option has the type `t:execution_level/0`.

  If the execution level is `:single_connection`, Xandra doens't inject any additional
  options.

  If the execution level is `:cluster`, Xandra injects these options when calling `c:new/1`:

    * `:connected_hosts` — a list of `{connection_pid, host}` tuples, where `connection_pid`
      (a `t:pid/0`) is the PID of the connection and `host` (a `t:Xandra.Cluster.Host.t/0`)
      is the corresponding host information. You can use this option to determine on which
      node to retry a query. In order to pick the a connection to use for the next attempt,
      inject the `:target_connection` option into the given options when you return them
      from `c:retry/3`. Elements in this list are ordered according to the
      `Xandra.Cluster.LoadBalancingPolicy` used by the cluster. If you want to keep track
      of the original `:connected_hosts`, you'll need to store them in the state of the
      retry strategy returned by `c:new/1`.

  ## Examples

  Let's look at some examples.

  ### Retry Count

  This is an example of a retry strategy that retries a fixed number of times
  before failing. It injects `:retry_count` option which it uses to keep track
  of how many times the query failed. This is effectively the `t:state/0` of this
  retry strategy.

      defmodule MyApp.CounterRetryStrategy do
        @behaviour Xandra.RetryStrategy

        @impl true
        def new(options) do
          # This is the "state" of this retry strategy
          Keyword.fetch!(options, :retry_count)
        end

        @impl true
        def retry(error, options, retries_left)

        def retry(_error, _options, _retries_left = 0) do
          :error
        end

        def retry(_error, options, retries_left = _state) do
          {:retry, options, retries_left - 1}
        end
      end

  ### Downgrading Consistency

  Another interesting example could be a retry strategy based on **downgrading
  consistency**: for example, we could execute all queries with a "high" consistency
  (such as `:all`) at first, and in case of failure, try again with a lower
  consistency (such as `:quorum`), finally giving up if that fails as well.

      defmodule MyApp.DowngradingConsistencyRetryStrategy do
        @behaviour Xandra.RetryStrategy

        @impl true
        def new(_options) do
          :no_state
        end

        @impl true
        def retry(_error, options, :no_state) do
          case Keyword.fetch(options, :consistency) do
            # No consistency was specified, so we don't bother to retry.
            :error -> :error

            # If the consistency was :all, we downgrade it by injecting a new one in the options.
            {:ok, :all} -> {:retry, Keyword.replace!(options, :consistency, :quorum), :no_state}

            # If the consistency was already lower than :all, we give up and stop retrying.
            {:ok, _other} -> :error
          end
        end
      end

  ### Different Node (for Clusters)

  A particularly-useful application of retry strategies is to retry queries on different hosts
  when using `Xandra.Cluster`. We can even choose not to execute on certain hosts
  (because they may be in a different data center). The following example retries on all hosts
  after the first `:connected_node` has failed:

      defmodule MyApp.AllNodesRetryStrategy do
        @behaviour Xandra.RetryStrategy

        alias Xandra.Cluster.Host

        @impl true
        def new(options) do
          if options[:execution_level] != :cluster do
            raise ArgumentError, "this retry strategy can only be used with clusters"
          end

          [_already_tried_node | remaining_nodes] = Keyword.fetch!(options, [:connected_hosts])
          remaining_nodes
        end

        @impl true
        def retry(error, options, nodes)

        # No nodes left to retry on.
        def retry(_error, options, [] = _remaining_nodes) do
          :error
        end

        # Try on the next node.
        def retry(_error, options, [{conn_pid, %Host{}} | remaining_nodes]) do
          options = Keyword.put(options, :target_connection, conn_pid)
          {:retry, options, remaining_nodes}
        end
      end

  """

  alias Xandra.Cluster.Host

  @typedoc """
  The possible values of the `:execution_level` option injected into the options
  passed to `c:new/1`.
  """
  @typedoc since: "0.18.0"
  @type execution_level() :: :connection | :cluster

  @typedoc """
  The state of the retry strategy.
  """
  @type state() :: term()

  @doc """
  Initializes the state of a retry strategy based on the given `options`.
  """
  @callback new(options :: keyword) :: state

  @doc """
  Determines whether to retry the failed query or return the error to the caller.

  The first argument is the **error** that caused the query to fail: for example, it
  could be a `Xandra.Error` struct with reason `:read_timeout`. This can be used
  to potentially determine the retry strategy based on the failure reason. The second
  argument is the options given to the function that failed while executing the
  query. The third argument is the retry strategy state returned either by
  `c:new/1` or by subsequent calls to `c:retry/3`.

  ## Return Values

  If `:error` is returned, the function that was trying to execute the query
  will return the error to the caller instead of retrying.

  If `{:retry, new_options, new_state}` is returned, the function that was
  trying to execute the query will be invoked again with the same query and
  `new_options` as its options. `new_state` will be used if the query fails
  again: in that case, `c:retry/3` will be invoked again with `new_state` as its
  third argument. This sequence of steps will repeat until either the query is executed
  successfully or this callback returns `:error`.
  """
  @callback retry(error :: term, options :: keyword, state) ::
              :error | {:retry, new_options :: keyword, new_state :: state}

  @doc false
  @spec run_with_retrying(keyword, (-> result)) :: result when result: var
  def run_with_retrying(options, fun) do
    options = Keyword.put(options, :execution_level, :xandra)

    case Keyword.pop(options, :retry_strategy) do
      {nil, _options} -> fun.()
      {retry_strategy, options} -> run_with_retrying(options, retry_strategy, fun)
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

        {nil, _options} ->
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
