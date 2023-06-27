defmodule Xandra.CCMTests.Test do
  use ExUnit.Case
  import ExUnit.CaptureLog

  setup context do
    Logger.configure(level: :debug)

    System.cmd("ccm", ["stop"])

    IO.puts("Starting Cassandra cluster for `#{context.test |> Atom.to_string()}`")
    {_output, exit_code} = start_ccm()
    assert exit_code == 0

    wait_for_nodes_up()

    on_exit(fn ->
      IO.puts("Stopping Cassandra cluster for `#{context.test |> Atom.to_string()}`")
      {elapsed_microsec, _} = :timer.tc(fn -> :os.cmd('ccm stop') end)
      IO.puts("Done in #{Float.round(elapsed_microsec / 1_000_000, 3)}s")
    end)

  end

  test "handles single node failure" do
    {:ok, conn} = Xandra.Cluster.start_link(
      nodes: ["127.0.0.1:9042", "127.0.0.2:9042", "127.0.0.3:9042", "127.0.0.4:9042"],
      protocol_version: :v4
    )
    Process.sleep(10000)

    expected_size = 4
    assert get_cluster_size(conn) == expected_size
    {up_nodes, down_nodes} = get_node_statuses(conn) |> IO.inspect()
    assert up_nodes |> length == 4
    assert down_nodes |> length == 0


    log = capture_log(fn ->
      {_output, _exit_code} = stop_ccm("node1")
      Process.sleep(1000)
      expected_size = 4
      assert get_cluster_size(conn) == expected_size
      {up_nodes, down_nodes} = get_node_statuses(conn)
      assert up_nodes |> length == 3
      assert down_nodes |> length == 1

    end)

    assert String.contains?(log, "Host marked as DOWN: 127.0.0.1:9042")
  end

  test "handles node recovery" do
    {:ok, conn} = Xandra.Cluster.start_link(
      nodes: ["127.0.0.1:9042", "127.0.0.2:9042", "127.0.0.3:9042", "127.0.0.4:9042"],
      protocol_version: :v4
    )
    Process.sleep(5000)

    expected_size = 4
    assert get_cluster_size(conn) == expected_size


    log = capture_log(fn ->
      {_output, _exit_code} = stop_ccm("node1")
      Process.sleep(1000)
      expected_size = 3
      assert get_cluster_size(conn) == expected_size
    end)

    assert String.contains?(log, "Host removed from the cluster: 127.0.0.1:9042")


    start_ccm("node1")
    Process.sleep(1000)

    expected_size = 4
    assert get_cluster_size(conn) == expected_size
  end

  test "handles connection interruptions" do
    {:ok, conn} = Xandra.Cluster.start_link(nodes: ["127.0.0.1:9042", "127.0.0.2:9042", "127.0.0.3:9042"])

    assert {:ok, _} = Xandra.execute(conn, "SELECT * FROM my_table")

    # Simulate connection interruption by stopping and starting the node immediately
    :os.cmd('ccm node1 stop')
    :os.cmd('ccm node1 start')

    :timer.sleep(5000)

    assert {:ok, _} = Xandra.execute(conn, "SELECT * FROM my_table")
  end

  def wait_for_nodes_up(timeout \\ 30000) do
    start_time = :erlang.monotonic_time()

    do_wait_for_nodes_up(start_time, timeout)
  end

  defp do_wait_for_nodes_up(start_time, timeout) do
    # Run the command and capture output
    {output, _exit_status} = System.cmd("ccm", ["status"])

    # Split the output into lines
    lines = String.split(output, "\n")

    # Check if all nodes are UP
    nodes_up = Enum.all?(lines, fn line ->
      not String.contains?(line, "DOWN")
    end)

    if nodes_up or :erlang.monotonic_time() - start_time >= timeout do
      Process.sleep(5000)
      nodes_up
    else
      # Sleep for a second before retrying
      :timer.sleep(1000)
      do_wait_for_nodes_up(start_time, timeout)
    end
  end

  def start_ccm(node_name \\ nil) do
    # Get the current username
    {username, _} = System.cmd("whoami", [])

    username = String.trim(username)

    command = if username == "root" do
      node_name =
        case node_name do
          nil -> ""
          _ -> node_name
        end

      ["ccm", Enum.join(List.flatten(["start", node_name, "--root"]), " ")]
    else
      case node_name do
        nil -> ["ccm", "start"]
        node -> ["ccm", "#{node} start"]
      end
    end

    System.cmd(Enum.at(command, 0), String.split(Enum.at(command, 1), " "))
  end

  def stop_ccm(node_name \\ nil) do
    case node_name do
      nil -> System.cmd("ccm", ["stop"])
      node ->
        System.cmd("ccm", ["#{node}", "stop"])
    end
  end


  def get_cluster_state(cluster_conn) do
    :sys.get_state(cluster_conn)
  end

  def get_cluster_size(cluster_conn) do
    state = get_cluster_state(cluster_conn)
    state.load_balancing_state |> length
  end

  def get_node_statuses(cluster_conn) do
    state = get_cluster_state(cluster_conn)
    state.load_balancing_state
    |> Enum.split_with(fn {_, status} -> status != :down end)
  end

end
