defmodule Xandra.TestHelper do
  import ExUnit.Assertions

  @spec await_connected(pid, keyword, (Xandra.conn() -> result), pos_integer) :: result
        when result: var
  def await_connected(cluster, options, fun, tries \\ 4) do
    Xandra.Cluster.run(cluster, options, fun)
  rescue
    Xandra.ConnectionError ->
      if tries > 0 do
        Process.sleep(50)
        await_connected(cluster, options, fun, tries - 1)
      else
        flunk("exceeded maximum number of attempts")
      end
  else
    {:error, %Xandra.ConnectionError{reason: {:cluster, :not_connected}}} ->
      if tries > 0 do
        Process.sleep(50)
        await_connected(cluster, options, fun, tries - 1)
      else
        flunk("exceeded maximum number of attempts")
      end

    other ->
      other
  end
end
