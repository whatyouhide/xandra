defmodule Mix.Tasks.Test.All do
  @moduledoc """
  A task for running tests on Cassandra and ScyllaDB systems.
  """
  use Mix.Task

  @shortdoc "Automatically run tests Cassandra and ScyllaDB"
  @preferred_cli_env :test

  def run(args \\ []) when is_list(args) do
    Mix.env(:test)

    if System.get_env("AUTHENTICATION") == "true" do
      Mix.Task.run("test.cassandra", ["--exclude", "test", "--include", "authentication"])
      Mix.Task.reenable("test")
      Mix.Task.run("test.scylla", ["--exclude", "test", "--include", "authentication"])
    else
      Mix.Task.run("test.cassandra")
      Mix.Task.reenable("test")
      Mix.Task.run("test.scylla")
    end
  end
end
