defmodule Xandra.Mixfile do
  use Mix.Project

  @description "Fast, simple, and robust Cassandra driver for Elixir."

  @repo_url "https://github.com/lexhide/xandra"

  @version "0.19.4"

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
        plt_local_path: "plts",
        plt_core_path: "plts"
      ],

      # Testing
      preferred_cli_env: [
        "test.cassandra": :test,
        "test.scylladb": :test,
        "test.all": :test,
        "test.all_with_html_coverage": :test,
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

  defp run_cassandra_tests(args, extra_env \\ []) do
    case List.keyfind(extra_env, "CASSANDRA_NATIVE_PROTOCOL", 0) do
      {_, protocol} -> print_header("Running Cassandra tests (native protocol #{protocol})")
      _other -> print_header("Running Cassandra tests")
    end

    mix_cmd_with_status_check(
      ["test", "--exclude", "scylla_specific", ansi_option() | args],
      [
        {"CASSANDRA_PORT", "9052"},
        {"CASSANDRA_WITH_AUTH_PORT", "9053"}
      ] ++ extra_env
    )
  end

  defp run_scylladb_tests(args, extra_env \\ []) do
    case List.keyfind(extra_env, "CASSANDRA_NATIVE_PROTOCOL", 0) do
      {_, protocol} -> print_header("Running ScyllaDB tests (native protocol #{protocol})")
      _other -> print_header("Running ScyllaDB tests")
    end

    mix_cmd_with_status_check(
      [
        "test",
        "--exclude",
        "cassandra_specific",
        "--exclude",
        "encryption",
        "--include",
        "scylla_specific",
        ansi_option() | args
      ],
      [
        {"CASSANDRA_PORT", "9062"},
        {"CASSANDRA_WITH_AUTH_PORT", "9063"}
      ] ++ extra_env
    )
  end

  defp run_tests_with_protocols_and_coverage(coverage_task, args) do
    for protocol <- ["", "v5", "v4", "v3"] do
      run_cassandra_tests(
        ["--cover", "--export-coverage", "cassandra-#{protocol}" | args],
        [{"CASSANDRA_NATIVE_PROTOCOL", protocol}]
      )
    end

    for protocol <- ["", "v4", "v3"] do
      run_scylladb_tests(
        ["--cover", "--export-coverage", "scylladb-#{protocol}" | args],
        [{"CASSANDRA_NATIVE_PROTOCOL", protocol}]
      )
    end

    Mix.Task.run(coverage_task, ["--exclude", "test", "--import-cover", "cover"])
  end

  defp aliases() do
    [
      test: "test --exclude scylla_specific",
      "test.cassandra": &run_cassandra_tests/1,
      "test.scylladb": &run_scylladb_tests/1,
      "test.all": fn args ->
        Mix.Task.run("test.cassandra", args)
        Mix.Task.run("test.scylladb", args)
      end,
      "test.all_with_html_coverage": &run_tests_with_protocols_and_coverage("coveralls.html", &1),
      "test.ci_with_coverage": &run_tests_with_protocols_and_coverage("coveralls.github", &1),
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
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.30", only: :dev},
      {:excoveralls, "~> 0.18", only: :test},
      {:mox, "~> 1.0", only: :test},
      {:stream_data, "~> 1.0", only: [:dev, :test]},
      {:nimble_lz4, "~> 0.1.3 or ~> 1.0", only: [:dev, :test]},
      # This is a fix to avoid warnings on Elixir 1.17+, it overrides the toxiproxy_ex
      # requirement.
      {:tesla, ">= 1.11.2", only: :test, override: true},
      {:toxiproxy_ex, "~> 2.0", only: :test}
    ]
  end

  defp mix_cmd_with_status_check(args, env) do
    port =
      Port.open({:spawn_executable, System.find_executable("mix")}, [
        :binary,
        :exit_status,
        args: args,
        env:
          Enum.map(env, fn {key, val} ->
            {String.to_charlist(key), String.to_charlist(val)}
          end)
      ])

    # We want a port so that we can shut down the port if we shut down the system.
    receive_loop(port)
  end

  defp receive_loop(port) do
    receive do
      {^port, {:data, data}} ->
        :ok = IO.write(data)
        receive_loop(port)

      {^port, {:exit_status, 0}} ->
        :ok

      {^port, {:exit_status, status}} ->
        Mix.raise("Mix failed with exit status #{status}")
    after
      60_000 ->
        Mix.raise("Timed out waiting for Mix to send back any data (after 60s)")
    end
  end

  defp ansi_option do
    if IO.ANSI.enabled?(), do: "--color", else: "--no-color"
  end

  defp print_header(header) do
    Mix.shell().info([:cyan, :bright, header, :reset])

    Mix.shell().info([
      :cyan,
      :bright,
      String.duplicate("=", String.length(header)) <> "\n",
      :reset
    ])
  end
end
