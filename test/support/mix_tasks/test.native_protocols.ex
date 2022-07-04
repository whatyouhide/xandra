defmodule Mix.Tasks.Test.NativeProtocols do
  use Mix.Task

  @supported_protocols [:auto] ++ Xandra.Frame.supported_protocols()

  @switches only_protocols: :string

  @impl true
  def run(args) do
    {opts_and_test_opts, test_args} = OptionParser.parse!(args, switches: @switches)
    {opts, test_opts} = Keyword.split(opts_and_test_opts, Keyword.keys(@switches))
    test_args = ["test"] ++ test_args ++ OptionParser.to_argv(test_opts)

    protocol_versions =
      case Keyword.fetch(opts, :only_protocols) do
        {:ok, protocols} ->
          protocols
          |> String.split(",")
          |> Enum.map(&String.to_existing_atom/1)
          |> Enum.map(fn
            protocol when protocol in @supported_protocols -> protocol
            other -> Mix.raise("Unknown protocol: #{inspect(other)}")
          end)

        :error ->
          @supported_protocols
      end

    Enum.each(protocol_versions, &run_tests(&1, test_args))
  end

  defp run_tests(protocol_version, test_args) do
    {_result, exit_status} =
      if protocol_version == :auto do
        Mix.shell().info([:cyan, "Testing with negotiated native protocol version", :reset])
        System.cmd("mix", test_args, stderr_to_stdout: false, into: IO.stream())
      else
        Mix.shell().info([
          :cyan,
          "Testing with forced native protocol version: ",
          :reset,
          inspect(protocol_version)
        ])

        System.cmd("mix", test_args,
          stderr_to_stdout: false,
          env: %{"CASSANDRA_NATIVE_PROTOCOL" => Atom.to_string(protocol_version)},
          into: IO.stream()
        )
      end

    if exit_status != 0 do
      System.halt(exit_status)
    end
  end
end
