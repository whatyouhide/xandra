defmodule Mix.Tasks.Test.Scylla do
  @moduledoc """
  A task for running tests on ScyllaDB systems.
  """
  use Mix.Task

  @shortdoc "Automatically run tests ScyllaDB"
  @preferred_cli_env :test

  def run(args \\ []) when is_list(args) do
    Mix.env(:test)

    exclude =
      (Keyword.take(args, [:exclude]) ++ [:cassandra_specific])
      |> Enum.map(&{:exclude, &1})

    include =
      (Keyword.take(args, [:include]) ++ [:scylla_spacific])
      |> Enum.map(&{:include, &1})

    arguments =
      (exclude ++ include)
      |> Enum.map(fn {type, value} ->
        case type do
          :include ->
            ["--include", "#{value}"]

          :exclude ->
            ["--exclude", "#{value}"]
        end
      end)
      |> List.flatten()

    import Logger, warn: false
    Logger.info("Start tests on ScyllaDB")
    Mix.Task.run("test", arguments)
  end
end
