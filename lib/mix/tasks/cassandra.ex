defmodule Mix.Tasks.Test.Cassandra do
  @moduledoc """
  A task for running tests on Cassandra systems.
  """
  use Mix.Task

  @shortdoc "Automatically run tests Cassandra"
  @preferred_cli_env :test

  def run(args \\ []) when is_list(args) do
    Mix.env(:test)

    import Logger, warn: false
    Logger.info("Start tests on Cassandra")
    Mix.Task.run("test", args ++ ["--exclude", "scylla_spacific"])
  end
end
