Code.require_file("docker_helpers.exs", __DIR__)

IO.puts("Running clustering tests")

Xandra.TestClustering.DockerHelpers.run!("docker-compose", [
  "-f",
  Xandra.TestClustering.DockerHelpers.docker_compose_file(),
  "build",
  "elixir"
])

Xandra.TestClustering.DockerHelpers.run!("docker-compose", [
  "-f",
  Xandra.TestClustering.DockerHelpers.docker_compose_file(),
  "run",
  "elixir",
  "mix",
  "run",
  "test_clustering/integration_test.exs"
])
