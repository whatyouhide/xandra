# This test case is responsible for *driving* testing of clustering by operating
# Docker Compose and running some tests inside an Elixir container running in Docker
# Compose as well.

defmodule Xandra.ClusterTest.CoordinationTest do
  use ExUnit.Case

  @docker_compose_file __DIR__
                       |> Path.join("docker-compose.cluster.yml")
                       |> Path.relative_to_cwd()

  setup_all do
    IO.puts("🚧 Starting Cassandra cluster with docker-compose up -d...")

    start_time = System.system_time()
    docker_compose!(["up", "-d", "--build", "--scale", "elixir=0"])

    wait_for_container_up("seed")
    wait_for_container_up("node1")
    wait_for_container_up("node2")
    wait_for_container_up("node3")

    on_exit(fn -> docker_compose!(["down"]) end)

    elapsed_ms =
      System.convert_time_unit(System.system_time() - start_time, :native, :millisecond)

    IO.puts("✅ Done in #{elapsed_ms / 1000}s")

    :ok
  end

  test "connect and discover peers" do
    docker_compose!([
      "run",
      "elixir",
      "mix",
      "test",
      "test/cluster_test/inside_docker/integration_test.exs",
      "--only",
      "connect_and_discover_peers"
    ])
  end

  defp docker_compose(args) do
    System.cmd("docker-compose", ["-f", @docker_compose_file | args], stderr_to_stdout: true)
  end

  defp docker_compose!(args) do
    case docker_compose(args) do
      {output, 0} ->
        output

      {output, exit_status} ->
        flunk("""
        docker-compose exited with status #{exit_status}. The command was:

          docker-compose #{Enum.join(args, " ")}

        The logged output was this:

        #{output}
        """)
    end
  end

  defp wait_for_container_up(name) do
    wait_for_container_up(name, _retry_interval = 1000, _timeout_left = 30_000)
  end

  defp wait_for_container_up(name, _retry_interval, timeout_left) when timeout_left <= 0 do
    flunk("Container is still not running or responding to health checks: #{inspect(name)}")
  end

  defp wait_for_container_up(name, retry_interval, timeout_left) do
    container_id = String.trim(docker_compose!(["ps", "-q", name]))

    {output, exit_status} =
      System.cmd("docker", [
        "inspect",
        "--format",
        "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        container_id
      ])

    assert exit_status == 0, "'docker inspect' failed with exit status #{exit_status}: #{output}"

    ip = String.trim(output)

    {output, exit_status} =
      docker_compose(["exec", "-T", name, "nodetool", "-h", "::FFFF:127.0.0.1", "status"])

    cond do
      exit_status == 0 ->
        if output
           |> String.split("\n")
           |> Enum.any?(&(&1 =~ ip and String.starts_with?(&1, "UN"))) do
          IO.puts("🌐 Cassandra node '#{name}' is up")
        else
          Process.sleep(retry_interval)
          wait_for_container_up(name, retry_interval, timeout_left - retry_interval)
        end

      output =~ "Has this node finished starting up?" and exit_status != 0 ->
        Process.sleep(retry_interval)
        wait_for_container_up(name, retry_interval, timeout_left - retry_interval)

      true ->
        flunk("Failed to get node status for node 'name':\n#{output}")
    end
  end
end
