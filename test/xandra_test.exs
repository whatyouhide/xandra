defmodule XandraTest do
  use XandraTest.IntegrationCase, async: true

  import XandraTest.IntegrationCase, only: [default_start_options: 0]

  alias Xandra.ConnectionError
  alias Xandra.Transport

  doctest Xandra

  describe "start_link/1" do
    test "validates the :nodes option" do
      assert_raise NimbleOptions.ValidationError, ~r{invalid node: "foo:bar"}, fn ->
        Xandra.start_link(nodes: ["foo:bar"])
      end

      assert_raise NimbleOptions.ValidationError, ~r{invalid list in :nodes option}, fn ->
        Xandra.start_link(nodes: [:not_even_a_string])
      end

      assert_raise ArgumentError, ~r{cannot use multiple nodes in the :nodes option}, fn ->
        Xandra.start_link(nodes: ["foo", "bar"])
      end

      # Empty list of nodes.
      assert_raise ArgumentError, ~r{the :nodes option can't be an empty list}, fn ->
        Xandra.start_link(nodes: [])
      end
    end

    test "validates the :authentication option" do
      assert_raise NimbleOptions.ValidationError, ~r{invalid value for :authentication}, fn ->
        Xandra.start_link(authentication: :nope)
      end
    end

    test "validates the :compressor option" do
      message =
        "invalid value for :compressor option: compressor module :nonexisting_module is not loaded"

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.start_link(compressor: :nonexisting_module)
      end

      message = ~r/expected compressor module to be a module/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.start_link(compressor: "not even an atom")
      end
    end

    test "returns an error if the connection is not established" do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [[:xandra, :failed_to_connect]])

      options = Keyword.merge(default_start_options(), nodes: ["nonexistent-domain"])

      conn = start_supervised!({Xandra, options})
      assert_receive {[:xandra, :failed_to_connect], ^telemetry_ref, %{}, %{connection: ^conn}}

      assert {:error, %ConnectionError{action: "check out connection", reason: :not_connected}} =
               Xandra.execute(conn, "USE some_keyspace")
    end

    test "supports the :connect_timeout option", %{start_options: start_options} do
      assert {:ok, conn} =
               start_supervised(
                 {Xandra, [connect_timeout: 0, backoff_type: :stop] ++ start_options}
               )

      ref = Process.monitor(conn)
      assert_receive {:DOWN, ^ref, _, _, reason}
      assert reason == :timeout
    end

    test "supports the :name option as an atom", %{start_options: start_options} do
      assert {:ok, conn} = start_supervised({Xandra, [name: :my_test_conn] ++ start_options})
      assert Process.whereis(:my_test_conn) == conn
    end

    test "supports the :name option as {:global, name}", %{start_options: start_options} do
      name = {:global, :my_global_test_conn}
      assert {:ok, conn} = start_supervised({Xandra, [name: name] ++ start_options})
      assert GenServer.whereis(name) == conn
    end

    test "supports the :name option as {:via, mod, term}",
         %{start_options: start_options, test: test_name} do
      registry_name = :"Registry_#{test_name}"
      start_supervised!({Registry, keys: :unique, name: registry_name})

      name = {:via, Registry, {registry_name, :my_via_test_conn}}
      assert {:ok, conn} = start_supervised({Xandra, [name: name] ++ start_options})
      assert GenServer.whereis(name) == conn
    end

    test "supports the :keyspace option", %{keyspace: keyspace, start_options: start_options} do
      assert {:ok, conn} = start_supervised({Xandra, [keyspace: keyspace] ++ start_options})

      assert {:connected, state} = :sys.get_state(conn)
      assert state.current_keyspace == keyspace
    end

    # Regression for https://github.com/lexhide/xandra/issues/266
    test "supports the :default_consistency option",
         %{keyspace: keyspace, start_options: start_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :execute_query, :stop]
        ])

      assert {:ok, test_conn} =
               start_supervised({Xandra, [default_consistency: :three] ++ start_options})

      Xandra.execute!(test_conn, "USE #{keyspace}", _params = [],
        telemetry_metadata: %{ref: telemetry_ref}
      )

      assert_receive {[:xandra, :execute_query, :stop], ^telemetry_ref, %{},
                      %{
                        connection: ^test_conn,
                        extra_metadata: %{ref: ^telemetry_ref},
                        query: query
                      }}

      assert %Xandra.Simple{} = query
      assert query.default_consistency == :three
    end

    test "supports the :configure option, as an anonymous function",
         %{start_options: start_options} do
      test_pid = self()
      ref = make_ref()

      configure_fun = fn options ->
        send(test_pid, {ref, options})
        Keyword.replace!(options, :nodes, start_options[:nodes])
      end

      modified_start_options =
        Keyword.merge(start_options, configure: configure_fun, nodes: ["localhost:9999"])

      assert {:ok, _test_conn} = start_supervised({Xandra, modified_start_options})

      assert_receive {^ref, configure_start_options}
      assert configure_start_options[:node] == {"localhost", 9999}
    end

    test "supports the :configure option, as a MFA tuple", %{start_options: start_options} do
      ref = make_ref()

      configure_fun = {__MODULE__, :configure_fun, [start_options, self(), ref]}

      modified_start_options =
        Keyword.merge(start_options, configure: configure_fun, nodes: ["localhost:9999"])

      assert {:ok, _test_conn} = start_supervised({Xandra, modified_start_options})

      assert_receive {^ref, configure_start_options}
      assert configure_start_options[:node] == {"localhost", 9999}
    end

    test "handles connection drops that happen right after connecting" do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :failed_to_connect]
        ])

      assert {:ok, listen_socket} = :gen_tcp.listen(0, active: false, mode: :binary)
      assert {:ok, port} = :inet.port(listen_socket)

      task =
        Task.async(fn ->
          # Accept a socket (so that Xandra's :gen_tcp.connect/3 call succeeds),
          # and then shut the socket down right away.
          assert {:ok, socket} = :gen_tcp.accept(listen_socket, :infinity)
          :ok = :gen_tcp.close(socket)
        end)

      assert {:ok, conn} = start_supervised({Xandra, nodes: ["localhost:#{port}"]})

      assert {:error, %ConnectionError{action: "check out connection", reason: :not_connected}} =
               Xandra.execute(conn, "SELECT * FROM system.local")

      assert :ok = Task.await(task)

      assert_received {[:xandra, :failed_to_connect], ^telemetry_ref, %{}, %{connection: ^conn}}
    end
  end

  describe "execute/3,4" do
    test "supports the :timeout option", %{conn: conn} do
      assert {:error, %ConnectionError{} = error} =
               Xandra.execute(conn, "SELECT * FROM system.local", [], timeout: 0)

      assert error.reason == :timeout
    end
  end

  describe "prepare/3" do
    test "supports the :timeout option", %{conn: conn} do
      assert {:error, %ConnectionError{} = error} =
               Xandra.prepare(conn, "SELECT * FROM system.local", timeout: 0)

      assert error.reason == :timeout
    end
  end

  describe "failure handling" do
    test "reconnects if the connection drops", %{start_options: start_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :connected],
          [:xandra, :disconnected]
        ])

      conn = start_supervised!({Xandra, start_options ++ [backoff_min: 0]})
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: ^conn}}

      assert {:ok, %Transport{} = transport} = Xandra.Connection.get_transport(conn)
      assert :ok = transport.module.shutdown(transport.socket, :read_write)

      assert_receive {[:xandra, :disconnected], ^telemetry_ref, %{}, %{connection: ^conn}}
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: ^conn}}
    end

    test "reconnects if the connection errors out", %{start_options: start_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :connected],
          [:xandra, :disconnected]
        ])

      conn = start_supervised!({Xandra, start_options ++ [backoff_min: 0]})
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: ^conn}}

      # Yuck, we have to send the connection a fake TCP error message here.
      assert {:ok, %Transport{} = transport} = Xandra.Connection.get_transport(conn)
      send(conn, {:tcp_error, transport.socket, :econnrefused})

      assert_receive {[:xandra, :disconnected], ^telemetry_ref, %{}, %{connection: ^conn}}
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: ^conn}}
    end
  end

  def configure_fun(options, original_start_options, pid, ref) do
    send(pid, {ref, options})
    Keyword.replace!(options, :nodes, original_start_options[:nodes])
  end
end
