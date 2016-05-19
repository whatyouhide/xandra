defmodule Xandra.Mixfile do
  use Mix.Project

  def project() do
    [app: :xandra,
     version: "0.0.1",
     elixir: "~> 1.2",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application() do
    [applications: [:logger]]
  end

  defp deps() do
    [{:db_connection, "~> 1.0-rc.5"}]
  end
end
