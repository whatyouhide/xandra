if System.get_env("XANDRA_DEBUG") do
  Xandra.Telemetry.attach_default_handler()
  Xandra.Telemetry.attach_debug_handler()
end

Logger.configure(level: String.to_existing_atom(System.get_env("LOG_LEVEL", "info")))

excluded =
  case XandraTest.IntegrationCase.protocol_version() do
    :v3 -> [:skip_for_native_protocol_v3]
    :v4 -> [:skip_for_native_protocol_v4]
    :v5 -> [:skip_for_native_protocol_v5]
    nil -> [:skip_for_native_protocol_v4, :skip_for_native_protocol_v3]
  end

Mox.defmock(LBPMock, for: Xandra.Cluster.LoadBalancingPolicy)

ExUnit.start(exclude: excluded, assert_receive_timeout: 1_000)
