defmodule TelemetryTest do
  use XandraTest.IntegrationCase, async: false

  import Xandra.TestHelper, only: [mirror_telemetry_event: 1]

  alias Xandra.Prepared

  setup_all %{setup_conn: conn, keyspace: keyspace} do
    Xandra.execute!(conn, "CREATE TABLE #{keyspace}.names (name text PRIMARY KEY)")
    :ok
  end

  describe "connection" do
    test "sends event on connection/disconnection" do
      mirror_telemetry_event([:xandra, :connected])
      start_supervised!({Xandra, [name: :telemetry_test_connection, pool_size: 1]})

      assert_receive {:telemetry_event, [:xandra, :connected], measurements, metadata}

      assert measurements == %{}
      assert metadata.connection_name == :telemetry_test_connection
      assert metadata.address == '127.0.0.1'
      assert metadata.port == 9042
    end
  end

  test "prepared query cache", %{conn: conn} do
    mirror_telemetry_event([:xandra, :prepared_cache, :hit])
    mirror_telemetry_event([:xandra, :prepared_cache, :miss])

    statement = "SELECT * FROM names"
    assert {:ok, prepared} = Xandra.prepare(conn, statement)

    assert_receive {:telemetry_event, [:xandra, :prepared_cache, :miss], %{query: %Prepared{}}, %{}}

    # Successive call to prepare uses cache.
    assert {:ok, ^prepared} = Xandra.prepare(conn, statement)

    assert_receive {:telemetry_event, [:xandra, :prepared_cache, :hit], %{query: %Prepared{}}, %{}}

    assert {:ok, ^prepared} = Xandra.prepare(conn, statement, force: true)
    assert_receive {:telemetry_event, [:xandra, :prepared_cache, :hit], %{query: %Prepared{}}, %{}}
  end

  test "prepare query", %{conn: conn} do
    mirror_telemetry_event([:xandra, :prepare_query, :start])
    mirror_telemetry_event([:xandra, :prepare_query, :stop])

    statement = "SELECT name FROM names"
    assert {:ok, %Prepared{}} = Xandra.prepare(conn, statement, telemetry_metadata: %{foo: :bar})

    assert_receive {:telemetry_event, [:xandra, :prepare_query, :start], %{system_time: system_time}, metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == '127.0.0.1'
    assert metadata.port == 9042
    assert metadata.extra_metadata == %{foo: :bar}
    assert is_integer(system_time)

    assert_receive {:telemetry_event, [:xandra, :prepare_query, :stop], %{duration: duration}, metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == '127.0.0.1'
    assert metadata.port == 9042
    assert metadata.extra_metadata == %{foo: :bar}
    assert metadata.reprepared == false
    assert is_integer(duration)

    assert {:ok, %Prepared{}} = Xandra.prepare(conn, statement, telemetry_metadata: %{foo: :bar}, force: true)

    assert_receive {:telemetry_event, [:xandra, :prepare_query, :stop], %{duration: duration}, metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == '127.0.0.1'
    assert metadata.port == 9042
    assert metadata.extra_metadata == %{foo: :bar}
    assert metadata.reprepared == true
    assert is_integer(duration)
  end

  test "execute_query", %{conn: conn} do
    mirror_telemetry_event([:xandra, :execute_query, :start])
    mirror_telemetry_event([:xandra, :execute_query, :stop])

    statement = "insert into names (name) values ('bob')"
    assert {:ok, %Xandra.Void{}} = Xandra.execute(conn, statement, [], telemetry_metadata: %{foo: :bar})

    assert_receive {:telemetry_event, [:xandra, :execute_query, :start], %{system_time: system_time}, metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == '127.0.0.1'
    assert metadata.port == 9042
    assert metadata.extra_metadata == %{foo: :bar}
    assert is_integer(system_time)

    assert_receive {:telemetry_event, [:xandra, :execute_query, :stop], %{duration: duration}, metadata}

    assert metadata.query.statement == statement
    assert metadata.connection_name == nil
    assert metadata.address == '127.0.0.1'
    assert metadata.port == 9042
    assert metadata.extra_metadata == %{foo: :bar}
    assert is_integer(duration)
  end
end
