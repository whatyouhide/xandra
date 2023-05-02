Logger.configure(level: String.to_existing_atom(System.get_env("LOG_LEVEL", "info")))

excluded =
  case XandraTest.IntegrationCase.protocol_version() do
    :v3 -> [:skip_for_native_protocol_v3]
    :v4 -> [:skip_for_native_protocol_v4]
    :v5 -> [:skip_for_native_protocol_v5]
    nil -> [:skip_for_native_protocol_v4, :skip_for_native_protocol_v3]
  end

# TODO: remove this conditional once we require Elixir 1.11+
# We only support the nimble_lz4 library for Elixir 1.11+.
excluded =
  if Version.match?(System.version(), "~> 1.11") do
    excluded
  else
    [:compression] ++ excluded
  end

ExUnit.start(exclude: excluded, assert_receive_timeout: 1_000)

Xandra.Telemetry.attach_default_handler()
