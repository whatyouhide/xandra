defmodule Mix.Tasks.Test.Scylla do
  @moduledoc """
  A task for running tests on ScyllaDB systems.
  """
  use Mix.Task

  @shortdoc "Automatically run tests ScyllaDB"
  @preferred_cli_env :test

  def run(args \\ []) when is_list(args) do
    Mix.env(:test)

    import Logger, warn: false
    Logger.info("Start tests on ScyllaDB")

    Mix.Task.run(
      "test",
      args ++ ["--exclude", "cassandra_specific", "--include", "scylla_spacific"]
    )
  end
end
