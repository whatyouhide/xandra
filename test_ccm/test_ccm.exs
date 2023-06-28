defmodule Xandra.CCMTests.Test do
  use ExUnit.Case
  import ExUnit.CaptureLog

  setup context do
    nodes_to_start =
      case context do
        %{tag: :three_nodes} ->
          :three
        _ ->
          :all
      end

    Logger.configure(level: :debug)

    System.cmd("ccm", ["stop"])
    System.cmd("pkill", ["-f", "socat"])

    IO.puts(
      "Starting Cassandra cluster with #{inspect(nodes_to_start)} nodes for `#{context.test |> Atom.to_string()}`"
    )

    {_output, exit_code} =
      case nodes_to_start do
        :all ->
          start_ccm()

        :three ->
          Enum.map(1..3, fn i ->
            start_ccm("node#{i}")
          end)
          |> List.first()
      end

    assert exit_code == 0

    wait_for_nodes_up()

    on_exit(fn ->
      IO.puts("Stopping Cassandra cluster for `#{context.test |> Atom.to_string()}`")
      {elapsed_microsec, _} = :timer.tc(fn -> :os.cmd('ccm stop') end)
      IO.puts("Done in #{Float.round(elapsed_microsec / 1_000_000, 3)}s")
    end)
  end

  test "handles single node failure" do
    {:ok, conn} =
      Xandra.Cluster.start_link(
        nodes: ["127.0.0.1:9042", "127.0.0.2:9042", "127.0.0.3:9042", "127.0.0.4:9042"],
        protocol_version: :v4
      )

    Process.sleep(10000)

    expected_size = 4
    assert get_cluster_size(conn) == expected_size
    {up_nodes, down_nodes} = get_node_statuses(conn)
    assert up_nodes |> length == 4
    assert down_nodes |> length == 0

    log =
      capture_log(fn ->
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
    {:ok, conn} =
      Xandra.Cluster.start_link(
        nodes: ["127.0.0.1:9042", "127.0.0.2:9042", "127.0.0.3:9042", "127.0.0.4:9042"],
        protocol_version: :v4
      )

    Process.sleep(5000)

    expected_size = 4
    assert get_cluster_size(conn) == expected_size
    {up_nodes, down_nodes} = get_node_statuses(conn)
    assert up_nodes |> length == 4
    assert down_nodes |> length == 0

    log =
      capture_log(fn ->
        stop_ccm("node1")
        Process.sleep(1000)

        {up_nodes, down_nodes} = get_node_statuses(conn)
        assert up_nodes |> length == 3
        assert down_nodes |> length == 1

        start_ccm("node1")
        Process.sleep(5000)
        assert get_cluster_size(conn) == expected_size
        {up_nodes, down_nodes} = get_node_statuses(conn)
        assert up_nodes |> length == 4
        assert down_nodes |> length == 0
      end)

    assert String.contains?(log, "Host marked as DOWN: 127.0.0.1:9042")
    assert String.contains?(log, "Host reported as UP: 127.0.0.1:9042")
  end

  test "dead node, but still reachable IP", _context do
    nodes = ["127.0.0.1:9042", "127.0.0.2:9042", "127.0.0.3:9042", "127.0.0.4:9042"]
    {:ok, conn} = Xandra.Cluster.start_link(nodes: nodes, protocol_version: :v4)
    Process.sleep(5000)

    log =
      capture_log(fn ->
        stop_ccm("node4")
      end)

    assert String.contains?(log, "Host marked as DOWN: 127.0.0.4:9042")

    pid = start_dummy_server(4)

    on_exit(fn ->
      Process.exit(pid, :kill)
      System.cmd("pkill", ["-f", "socat"])
    end)

    Process.sleep(1000)
    assert port_open?('127.0.0.4')

    # Check if the port is open
    assert port_open?({127, 0, 0, 4}, 9042)

    expected_size = 4
    assert get_cluster_size(conn) == expected_size
    {up_nodes, down_nodes} = get_node_statuses(conn)
    assert up_nodes |> length == 3
    assert down_nodes |> length == 1
  end

  @tag :three_nodes
  test "reachable IP but was never a node", _context do
    pid = start_dummy_server(4)
    Process.sleep(1000)
    assert port_open?('127.0.0.4')

    on_exit(fn ->
      Process.exit(pid, :kill)
      System.cmd("pkill", ["-f", "socat"])
    end)

    nodes = ["127.0.0.1:9042", "127.0.0.2:9042", "127.0.0.3:9042", "127.0.0.5:9042"]
    {:ok, conn} = Xandra.Cluster.start_link(nodes: nodes, protocol_version: :v4)
    Process.sleep(5000)

    expected_size = 4
    assert get_cluster_size(conn) == expected_size
    {up_nodes, down_nodes} = get_node_statuses(conn)
    assert up_nodes |> length == 3 # should think .5 is a potential candidate
    assert down_nodes |> length == 1 # knows that .4 is down
  end

  @tag :three_nodes
  test "new node", _context do
    nodes = ["127.0.0.1:9042", "127.0.0.2:9042", "127.0.0.3:9042"]
    {:ok, conn} = Xandra.Cluster.start_link(nodes: nodes, protocol_version: :v4)
    Process.sleep(5000)
    start_ccm("node4")

    expected_size = 4
    assert get_cluster_size(conn) == expected_size
    {up_nodes, down_nodes} = get_node_statuses(conn)
    assert up_nodes |> length == 4
    assert down_nodes |> length == 0
  end

  @tag :three_nodes
  test "new node + dies right away", _context do
    nodes = ["127.0.0.1:9042", "127.0.0.2:9042", "127.0.0.3:9042"]
    {:ok, conn} = Xandra.Cluster.start_link(nodes: nodes, protocol_version: :v4)
    Process.sleep(5000)

    start_ccm("node4")
    Process.sleep(5000)
    stop_ccm("node4")

    expected_size = 4
    assert get_cluster_size(conn) == expected_size
    {up_nodes, down_nodes} = get_node_statuses(conn)
    assert up_nodes |> length == 3
    assert down_nodes |> length == 1
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
    nodes_up =
      Enum.all?(lines, fn line ->
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

    command =
      if username == "root" do
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
      nil ->
        System.cmd("ccm", ["stop"])

      node ->
        System.cmd("ccm", ["#{node}", "stop"])
    end
  end

  # Dials a TCP port to see if it's open
  defp port_open?(ip, port \\ 9042) do
    case :gen_tcp.connect(ip, port, [:binary, active: false]) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      {:error, _} ->
        false
    end
  end

  # runs `socat TCP-LISTEN:<PORT>,fork,bind=127.0.0.<IP> /dev/null` to start a dummy TCP listener
  def start_dummy_server(host_identifier, port \\ 9042) do
    spawn_link(fn ->
      System.cmd("socat", ["TCP-LISTEN:#{port},fork,bind=127.0.0.#{host_identifier}", "/dev/null"])
    end)
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
