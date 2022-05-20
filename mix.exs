defmodule Xandra.Mixfile do
  use Mix.Project

  @description "Fast, simple, and robust Cassandra driver for Elixir."

  @repo_url "https://github.com/lexhide/xandra"

  @version "0.14.0"

  def project() do
    [
      app: :xandra,
      version: @version,
      elixir: "~> 1.9",
      elixirc_paths: elixirc_paths(Mix.env()),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Task aliases
      aliases: aliases(),

      # Dialyzer
      dialyzer: [flags: [:no_contracts]],

      # Testing
      preferred_cli_env: [
        "test.scylladb": :test,
        "test.clustering": :test,
        "coveralls.html": :test
      ],
      test_coverage: [tool: ExCoveralls],

      # Hex
      package: package(),
      description: @description,

      # Docs
      name: "Xandra",
      docs: [
        main: "Xandra",
        source_ref: "v#{@version}",
        source_url: @repo_url,
        extras: [
          "pages/Data types comparison table.md"
        ]
      ]
    ]
  end

  def application() do
    [extra_applications: [:logger]]
  end

  defp elixirc_paths(:test), do: ["test/support"] ++ elixirc_paths(:dev)
  defp elixirc_paths(_env), do: ["lib"]

  defp package() do
    [
      maintainers: ["Aleksei Magusev", "Andrea Leopardi"],
      licenses: ["ISC"],
      links: %{"GitHub" => @repo_url}
    ]
  end

  defp aliases() do
    [
      "test.scylladb": "test --exclude encryption --exclude cassandra_specific",
      "test.clustering": "run test_clustering/run.exs"
    ]
  end

  defp deps() do
    [
      {:db_connection, "~> 2.0"},
      {:decimal, "~> 1.7", optional: true},
      {:nimble_options, "~> 0.4.0"},

      # Dev and test dependencies
      {:ex_doc, "~> 0.28", only: :dev},
      {:dialyxir, "~> 1.1", only: :dev, runtime: false},
      {:excoveralls, "~> 0.14.4", only: :test},
      {:nimble_lz4, github: "whatyouhide/nimble_lz4", only: :test},
      {:stream_data, "~> 0.5.0", only: :test}
    ]
  end
end
