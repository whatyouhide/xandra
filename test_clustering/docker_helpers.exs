defmodule Xandra.TestClustering.DockerHelpers do
  import ExUnit.Assertions

  def docker_compose_file() do
    Path.join(__DIR__, "docker-compose.cluster.yml")
  end

  def run!(cmd, args) do
    {_output, exit_status} =
      System.cmd(cmd, args, stderr_to_stdout: true, into: IO.stream(:stdio, :line))

    if exit_status != 0 do
      flunk("""
      Command exited with non-zero status #{exit_status}:

        #{cmd} #{Enum.join(args, " ")}
      """)
    end
  end

  def docker_compose(args) do
    System.cmd("docker", ["compose", "-f", docker_compose_file() | args], stderr_to_stdout: true)
  end

  def docker_compose!(args) do
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

  def wait_for_container_up(name) do
    wait_for_container_up(name, _retry_interval = 1000, _timeout_left = 30_000)
  end

  def wait_for_container_up(name, _retry_interval, timeout_left) when timeout_left <= 0 do
    flunk("Container is still not running or responding to health checks: #{inspect(name)}")
  end

  def wait_for_container_up(name, retry_interval, timeout_left) do
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
        wait_for_container_up(name, retry_interval, timeout_left - retry_interval)
    end
  end
end
