defmodule Mix.Tasks.Test.Cassandra do
  @moduledoc """
  A task for running tests on Cassandra systems.
  """
  use Mix.Task

  @shortdoc "Automatically run tests Cassandra"
  @preferred_cli_env :test

  def run(args \\ []) when is_list(args) do
    Mix.env(:test)

    exclude =
      (Keyword.take(args, [:exclude]) ++ [:scylla_spacific])
      |> Enum.map(&{:exclude, &1})

    include =
      (Keyword.take(args, [:include]) ++ [])
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
    Logger.info("Start tests on Cassandra")
    Mix.Task.run("test", arguments)
  end
end
