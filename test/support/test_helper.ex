defmodule Xandra.TestHelper do
  import ExUnit.Assertions

  require Logger

  @spec await_cluster_connected(pid, pos_integer) :: :ok
  def await_cluster_connected(cluster, tries \\ 10) when is_pid(cluster) do
    fun = &Xandra.execute!(&1, "SELECT * FROM system.local")

    case Xandra.Cluster.run(cluster, _options = [], fun) do
      {:error, %Xandra.ConnectionError{} = error} -> raise error
      _other -> :ok
    end
  rescue
    Xandra.ConnectionError ->
      if tries > 0 do
        Process.sleep(100)
        await_cluster_connected(cluster, tries - 1)
      else
        flunk("exceeded maximum number of attempts")
      end
  end

  # TODO: remove once we have ExUnit.Callbacks.start_link_supervised!/1 (Elixir 1.14+).
  if function_exported?(ExUnit.Callbacks, :start_link_supervised!, 2) do
    defdelegate start_link_supervised!(spec), to: ExUnit.Callbacks
    defdelegate start_link_supervised!(spec, opts), to: ExUnit.Callbacks
  else
    @spec start_link_supervised!(Supervisor.child_spec(), keyword) :: pid
    def start_link_supervised!(spec, opts \\ []) do
      pid = ExUnit.Callbacks.start_supervised!(spec, opts)
      true = Process.link(pid)
      pid
    end
  end

  @spec wait_for_passing(pos_integer, (() -> result)) :: result when result: var
  def wait_for_passing(time_left, fun)

  def wait_for_passing(time_left, fun) when time_left < 0, do: fun.()

  @sleep_interval 50
  def wait_for_passing(time_left, fun) do
    fun.()
  catch
    _, _ ->
      Process.sleep(@sleep_interval)
      wait_for_passing(time_left - @sleep_interval, fun)
  end

  @spec cmd!(String.t(), [String.t()]) :: String.t()
  def cmd!(cmd, args) do
    Logger.debug("Running command: #{cmd} #{Enum.map_join(args, " ", &format_arg/1)}")

    case System.cmd(cmd, args, stderr_to_stdout: true) do
      {output, 0} ->
        if output != "" do
          Logger.debug("Command output: #{String.trim(output)}")
        end

        output

      {output, code} ->
        flunk("""
        Command failed with exit code #{code}. The command was:

          #{cmd} #{Enum.map_join(args, " ", &format_arg/1)}

        The output was:

        #{output}
        """)
    end
  end

  defp format_arg(arg) do
    if String.contains?(arg, " ") do
      inspect(arg)
    else
      arg
    end
  end
end
