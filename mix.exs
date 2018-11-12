defmodule Xandra.Mixfile do
  use Mix.Project

  @description "Fast, simple, and robust Cassandra driver for Elixir."

  @repo_url "https://github.com/lexhide/xandra"

  @version "0.10.0"

  def project() do
    [
      app: :xandra,
      version: @version,
      elixir: "~> 1.4",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Tests
      aliases: ["test.scylladb": &test_scylladb/1, "test.all": ["test", "test.scylladb"]],
      preferred_cli_env: ["test.scylladb": :test, "test.all": :test],

      # Hex
      package: package(),
      description: @description,

      # Docs
      name: "Xandra",
      docs: [main: "Xandra", source_ref: "v#{@version}", source_url: @repo_url]
    ]
  end

  def application() do
    [applications: [:logger, :db_connection]]
  end

  defp package() do
    [
      maintainers: ["Aleksei Magusev", "Andrea Leopardi"],
      licenses: ["ISC"],
      links: %{"GitHub" => @repo_url}
    ]
  end

  defp deps() do
    [
      {:db_connection, "~> 1.0"},
      {:snappy, github: "skunkwerks/snappy-erlang-nif", only: [:dev, :test]},
      {:ex_doc, "~> 0.14", only: :dev}
    ]
  end

  defp test_scylladb(args) do
    args = if IO.ANSI.enabled?(), do: ["--color" | args], else: ["--no-color" | args]

    Mix.shell().info("Running ScyllaDB tests")

    {_, res} =
      System.cmd("mix", ["test", "--exclude", "cassandra_specific" | args],
        into: IO.binstream(:stdio, :line),
        env: [{"PORT", "9043"}]
      )

    if res > 0 do
      System.at_exit(fn _ -> exit({:shutdown, 1}) end)
    end
  end
end
