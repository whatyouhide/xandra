defmodule ScyllaTest do
  start_options = [nodes: ["127.0.0.1:9043"]]

  use XandraTest.IntegrationCase, start_options: start_options

  @moduletag :scylla_spacific
end
