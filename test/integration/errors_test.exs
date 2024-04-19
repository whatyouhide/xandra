defmodule ErrorsTest do
  use XandraTest.IntegrationCase, async: true

  alias Xandra.Error
  alias Xandra.Cluster
  alias Xandra.Cluster.Host
  alias Xandra.ConnectionError

  test "each possible error", %{conn: conn} do
    assert {:error, reason} = Xandra.execute(conn, "")
    assert %Error{reason: :invalid_syntax} = reason

    assert {:error, reason} = Xandra.execute(conn, "USE unknown")
    assert %Error{reason: :invalid} = reason

    Xandra.execute!(conn, "CREATE TABLE errors (id int PRIMARY KEY, reason text)")
    assert {:error, reason} = Xandra.execute(conn, "CREATE TABLE errors (id int PRIMARY KEY)")
    assert %Error{reason: :already_exists} = reason

    assert {:error, reason} = Xandra.prepare(conn, "SELECT * FROM unknown")
    assert %Error{reason: :invalid} = reason
  end

  @tag :cassandra_specific
  @tag :skip_for_native_protocol_v3
  @tag start_conn: false
  test "function_failure error", %{keyspace: keyspace, start_options: start_options} do
    # This is only supported in native protocol v4.
    start_options = Keyword.put(start_options, :protocol_version, :v4)

    conn = start_supervised!({Xandra, start_options})
    Xandra.execute!(conn, "USE #{keyspace}")
    Xandra.execute!(conn, "CREATE TABLE funs (id int PRIMARY KEY, name text)")

    Xandra.execute!(conn, """
    CREATE FUNCTION thrower (x int) CALLED ON NULL INPUT
    RETURNS int LANGUAGE java AS 'throw new RuntimeException();'
    """)

    Xandra.execute!(conn, "INSERT INTO funs (id, name) VALUES (1, 'my_fun')")

    assert {:error, %Error{} = error} = Xandra.execute(conn, "SELECT thrower(id) FROM funs")
    assert error.reason == :function_failure
    assert error.message =~ "java.lang.RuntimeException"
  end

  test "errors are raised from bang! functions", %{conn: conn} do
    assert_raise Error, fn -> Xandra.prepare!(conn, "") end
    assert_raise Error, fn -> Xandra.execute!(conn, "USE unknown") end
  end

  describe "on Xandra.Cluster level" do
    defmodule Xandra.Cluster.PoolMock do
      @behaviour :gen_statem

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :worker,
          restart: :permanent
        }
      end

      def start_link([]) do
        :gen_statem.start_link(__MODULE__, :no_args, [])
      end

      def checkout(pid) do
        :gen_statem.call(pid, :checkout)
      end

      @impl true
      def init(:no_args) do
        {dead_pid, ref} = spawn_monitor(fn -> :ok end)

        receive do
          {:DOWN, ^ref, _, _, _} -> :ok
        end
        {:ok, :waiting, [{random_pid, %Host{}}]}
      end

      @impl true
      def callback_mode do
        :state_functions
      end

      def waiting({:call, from}, :checkout, data) do
        {:keep_state_and_data, {:reply, from, {:ok, data}}}
      end
    end

    test "noproc errors are caught" do
      {:ok, cluster} = start_supervised(Xandra.Cluster.PoolMock)

      assert {:error, %ConnectionError{action: "execute", reason: {:cluster, :pool_closed}}} =
               Cluster.execute(cluster, "select * from system.peers")
    end
  end
end
