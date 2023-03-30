import Config

config :logger, :console,
  format: "[$level] $metadata $message\n",
  metadata: [:xandra_address, :xandra_port, :module]
