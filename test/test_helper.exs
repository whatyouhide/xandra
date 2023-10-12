if System.get_env("XANDRA_DEBUG") do
  Xandra.Telemetry.attach_default_handler()
  Xandra.Telemetry.attach_debug_handler()
end

ToxiproxyEx.populate!([
  %{
    name: "xandra_test_cassandra",
    listen: "0.0.0.0:19052",
    upstream: "cassandra:9042"
  }
])

Logger.configure(level: String.to_existing_atom(System.get_env("LOG_LEVEL", "info")))

excluded =
  case XandraTest.IntegrationCase.protocol_version() do
    :v3 -> [:skip_for_native_protocol_v3]
    :v4 -> [:skip_for_native_protocol_v4]
    :v5 -> [:skip_for_native_protocol_v5]
    nil -> [:skip_for_native_protocol_v4, :skip_for_native_protocol_v3]
  end

excluded =
  if System.find_executable("ccm") do
    excluded
  else
    message = """
    ccm was not found in your PATH. Xandra requires it in order to run cluster tests, so \
    we're skipping ccm-based tests for now. Make sure you can run "ccm" in your shell.\
    """

    IO.puts(IO.ANSI.format([:yellow, "WARNING: ", :reset, message, ?\n]))
    excluded ++ [:ccm]
  end

Mox.defmock(LBPMock, for: Xandra.Cluster.LoadBalancingPolicy)

ExUnit.start(exclude: excluded, assert_receive_timeout: 1_000)
