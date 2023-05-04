defmodule Xandra.Mixfile do
  use Mix.Project

  @description "Fast, simple, and robust Cassandra driver for Elixir."

  @repo_url "https://github.com/lexhide/xandra"

  @version "0.15.0-rc.1"

  def project() do
    [
      app: :xandra,
      version: @version,
      elixir: "~> 1.10",
      elixirc_paths: elixirc_paths(Mix.env()),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      xref: [exclude: [Decimal]],

      # Task aliases
      aliases: aliases(),

      # Dialyzer
      dialyzer: [
        flags: [:no_contracts, :no_improper_lists],
        plt_add_apps: [:ssl, :crypto, :mix, :ex_unit, :decimal]
      ],

      # Testing
      preferred_cli_env: [
        "test.scylladb": :test,
        "test.clustering": :test,
        "test.native_protocols": :test,
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
          "pages/Data types comparison table.md",
          "pages/Compatibility.md"
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
      maintainers: ["Andrea Leopardi"],
      licenses: ["ISC"],
      links: %{"GitHub" => @repo_url}
    ]
  end

  defp aliases() do
    [
      "test.scylladb": "test --exclude cassandra_specific --exclude encryption",
      "test.clustering": "run test_clustering/run.exs"
    ]
  end

  defp deps() do
    [
      {:db_connection, "~> 2.0"},
      {:decimal, "~> 1.7 or ~> 2.0", optional: true},
      {:nimble_options, "~> 0.5.0 or ~> 1.0"},
      {:telemetry, "~> 0.4.3 or ~> 1.0"},

      # Dev and test dependencies
      {:ex_doc, "~> 0.28", only: :dev},
      {:dialyxir, "~> 1.3", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.16.0", only: :test},
      {:stream_data, "~> 0.5.0", only: [:dev, :test]}
    ] ++
      if Version.match?(System.version(), "~> 1.11") do
        # TODO: remove this conditional once we require Elixir 1.11+
        [{:nimble_lz4, "~> 0.1.2", only: [:dev, :test]}]
      else
        []
      end
  end
end
