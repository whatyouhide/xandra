defmodule TelemetryTest do
  use XandraTest.IntegrationCase, async: false

  alias Xandra.Prepared

  setup_all %{setup_conn: conn, keyspace: keyspace} do
    Xandra.execute!(conn, "CREATE TABLE #{keyspace}.names (name text PRIMARY KEY)")
    :ok
  end

  describe "connection" do
    @tag start_conn: false
    test "sends event on connection/disconnection", %{start_options: start_options} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:xandra, :connected]])

      conn = start_supervised!({Xandra, [name: :telemetry_test_connection] ++ start_options})

      assert_receive {[:xandra, :connected], ^ref, measurements, %{connection: ^conn} = metadata}

      assert measurements == %{}
      assert metadata.connection_name == :telemetry_test_connection
      assert metadata.address == "127.0.0.1"
      assert metadata.port == XandraTest.IntegrationCase.port()
    end
  end

  test "prepared query cache", %{conn: conn} do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:xandra, :prepared_cache, :hit],
        [:xandra, :prepared_cache, :miss]
      ])

    statement = "SELECT * FROM names"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)

    assert_receive {[:xandra, :prepared_cache, :miss], ^ref, %{}, %{connection: ^conn} = metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == "127.0.0.1"
    assert metadata.port == XandraTest.IntegrationCase.port()

    # Successive call to prepare uses cache.
    assert {:ok, ^prepared} = Xandra.prepare(conn, statement)

    assert_receive {[:xandra, :prepared_cache, :hit], ^ref, %{}, %{connection: ^conn} = metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == "127.0.0.1"
    assert metadata.port == XandraTest.IntegrationCase.port()

    assert {:ok, ^prepared} = Xandra.prepare(conn, statement, force: true)

    assert_receive {[:xandra, :prepared_cache, :hit], ^ref, %{}, %{connection: ^conn} = metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == "127.0.0.1"
    assert metadata.port == XandraTest.IntegrationCase.port()
  end

  test "prepare query", %{conn: conn} do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:xandra, :prepare_query, :start],
        [:xandra, :prepare_query, :stop]
      ])

    statement = "SELECT name FROM names"
    assert %Prepared{} = Xandra.prepare!(conn, statement, telemetry_metadata: %{foo: :bar})

    assert_receive {[:xandra, :prepare_query, :start], ^ref, %{system_time: system_time},
                    %{connection: ^conn} = metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == "127.0.0.1"
    assert metadata.port == XandraTest.IntegrationCase.port()
    assert metadata.extra_metadata == %{foo: :bar}
    assert is_integer(system_time)

    assert_receive {[:xandra, :prepare_query, :stop], ^ref, %{duration: duration},
                    %{connection: ^conn} = metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == "127.0.0.1"
    assert metadata.port == XandraTest.IntegrationCase.port()
    assert metadata.extra_metadata == %{foo: :bar}
    assert metadata.reprepared == false
    assert is_integer(duration)

    assert {:ok, %Prepared{}} =
             Xandra.prepare(conn, statement, telemetry_metadata: %{foo: :bar}, force: true)

    assert_receive {[:xandra, :prepare_query, :stop], ^ref, %{duration: duration},
                    %{connection: ^conn} = metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == "127.0.0.1"
    assert metadata.port == XandraTest.IntegrationCase.port()
    assert metadata.extra_metadata == %{foo: :bar}
    assert metadata.reprepared == true
    assert is_integer(duration)
  end

  test "execute query", %{conn: conn} do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:xandra, :execute_query, :start],
        [:xandra, :execute_query, :stop]
      ])

    statement = "insert into names (name) values ('bob')"

    assert {:ok, %Xandra.Void{}} =
             Xandra.execute(conn, statement, [], telemetry_metadata: %{foo: :bar})

    assert_receive {[:xandra, :execute_query, :start], ^ref, %{system_time: system_time},
                    metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == "127.0.0.1"
    assert metadata.port == XandraTest.IntegrationCase.port()
    assert metadata.extra_metadata == %{foo: :bar}
    assert is_integer(system_time)

    assert_receive {[:xandra, :execute_query, :stop], ^ref, %{duration: duration}, metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == "127.0.0.1"
    assert metadata.port == XandraTest.IntegrationCase.port()
    assert metadata.extra_metadata == %{foo: :bar}
    assert is_integer(duration)
  end

  test "execute query with error", %{conn: conn} do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:xandra, :execute_query, :start],
        [:xandra, :execute_query, :stop]
      ])

    statement = "invalid syntax"

    assert {:error, %Xandra.Error{reason: :invalid_syntax}} =
             Xandra.execute(conn, statement, [], telemetry_metadata: %{foo: :bar})

    assert_receive {[:xandra, :execute_query, :start], ^ref, %{system_time: system_time},
                    metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == "127.0.0.1"
    assert metadata.port == XandraTest.IntegrationCase.port()
    assert metadata.extra_metadata == %{foo: :bar}
    assert is_integer(system_time)

    assert_receive {[:xandra, :execute_query, :stop], ^ref, %{duration: duration}, metadata}

    assert %Xandra.Error{reason: :invalid_syntax} = metadata.reason
    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == "127.0.0.1"
    assert metadata.port == XandraTest.IntegrationCase.port()
    assert metadata.extra_metadata == %{foo: :bar}
    assert is_integer(duration)
  end
end
