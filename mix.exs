defmodule Xandra.Mixfile do
  use Mix.Project

  @description "Fast, simple, and robust Cassandra driver for Elixir."

  @repo_url "https://github.com/lexhide/xandra"

  @version "0.18.0-rc.9"

  def project() do
    [
      app: :xandra,
      version: @version,
      elixir: "~> 1.15",
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
        list_unused_filters: true,
        plt_add_apps: [:ssl, :crypto, :mix, :ex_unit, :erts, :kernel, :stdlib],
        plt_local_path: "priv/plts",
        plt_core_path: "priv/plts"
      ],

      # Testing
      preferred_cli_env: [
        "test.scylladb": :test,
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
          "pages/Compatibility.md",
          "pages/Telemetry events.md"
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
      test: "test --exclude scylla_specific",
      "test.scylladb": [
        fn _args ->
          System.put_env("CASSANDRA_PORT", "9062")
          System.put_env("CASSANDRA_WITH_AUTH_PORT", "9063")
        end,
        "test --exclude cassandra_specific --exclude encryption --include scylla_specific"
      ],
      "test.all": fn args ->
        Mix.Task.run(:test, args)
        Mix.Task.run(:"test.scylladb", args)
      end,
      docs: [
        "run pages/generate_telemetry_events_page.exs",
        "docs"
      ]
    ]
  end

  defp deps() do
    [
      {:decimal, "~> 1.7 or ~> 2.0", optional: true},
      {:nimble_options, "~> 1.0"},
      {:telemetry, "~> 0.4.3 or ~> 1.0"},

      # Dev and test dependencies
      {:dialyxir, "~> 1.3", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.28", only: :dev},
      {:excoveralls, "~> 0.17", only: :test},
      {:mox, "~> 1.0", only: :test},
      {:stream_data, "~> 0.6.0", only: [:dev, :test]},
      {:nimble_lz4, "~> 0.1.3", only: [:dev, :test]},
      {:toxiproxy_ex, github: "whatyouhide/toxiproxy_ex", only: :test}
    ]
  end
end
