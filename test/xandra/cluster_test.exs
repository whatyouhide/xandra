defmodule Xandra.ClusterTest do
  use ExUnit.Case

  alias Xandra.Cluster
  alias Xandra.Cluster.TopologyChange

  import ExUnit.CaptureLog

  @moduletag capture_log: true

  describe "autodiscover_other_datacenters" do
    test "connects to new node in same datacenter when autodiscover_other_datacenters is false" do
      {:ok, cluster} =
        Cluster.start_link(
          nodes: ["127.0.0.1"],
          autodiscover_other_datacenters: false,
          autodiscovery: true
        )

      wait_for_connection(cluster)

      log =
        capture_log(fn ->
          Cluster.update(
            cluster,
            %TopologyChange{effect: "NEW_NODE", address: {192, 0, 2, 0}, port: 9042},
            "datacenter1"
          )

          wait_for_connection(cluster)
        end)

      assert log =~ "Started connection to {192, 0, 2, 0}"
    end

    test "doesn't connect to new node in different datacenter when autodiscover_other_datacenters is false" do
      {:ok, cluster} =
        Cluster.start_link(
          nodes: ["127.0.0.1"],
          autodiscover_other_datacenters: false,
          autodiscovery: true
        )

      wait_for_connection(cluster)

      log =
        capture_log(fn ->
          Cluster.update(
            cluster,
            %TopologyChange{effect: "NEW_NODE", address: {192, 0, 2, 0}, port: 9042},
            "datacenter2"
          )

          wait_for_connection(cluster)
        end)

      refute log =~ "Started connection to {192, 0, 2, 0}"
    end

    test "connects to new node in same datacenter when autodiscover_other_datacenters is true" do
      {:ok, cluster} =
        Cluster.start_link(
          nodes: ["127.0.0.1"],
          autodiscover_other_datacenters: true,
          autodiscovery: true
        )

      wait_for_connection(cluster)

      log =
        capture_log(fn ->
          Cluster.update(
            cluster,
            %TopologyChange{effect: "NEW_NODE", address: {192, 0, 2, 0}, port: 9042},
            "datacenter1"
          )

          wait_for_connection(cluster)
        end)

      assert log =~ "Started connection to {192, 0, 2, 0}"
    end

    test "connects to new node in different datacenter when autodiscover_other_datacenters is true" do
      {:ok, cluster} =
        Cluster.start_link(
          nodes: ["127.0.0.1"],
          autodiscover_other_datacenters: true,
          autodiscovery: true
        )

      wait_for_connection(cluster)

      log =
        capture_log(fn ->
          Cluster.update(
            cluster,
            %TopologyChange{effect: "NEW_NODE", address: {192, 0, 2, 0}, port: 9042},
            "datacenter2"
          )

          wait_for_connection(cluster)
        end)

      assert log =~ "Started connection to {192, 0, 2, 0}"
    end
  end

  describe "dc-aware load balancing" do
    test "prefers first node's data center" do
      {:ok, cluster} =
        Cluster.start_link(
          nodes: ["127.0.0.1"],
          autodiscover_other_datacenters: true,
          autodiscovery: true,
          load_balancing: :dc_aware
        )

      wait_for_connection(cluster)

      {:ok, inital_pool} = GenServer.call(cluster, :checkout)

      Cluster.update(
        cluster,
        %TopologyChange{effect: "NEW_NODE", address: {192, 0, 2, 0}, port: 9042},
        "datacenter2"
      )

      wait_for_connection(cluster)

      assert {:ok, ^inital_pool} = GenServer.call(cluster, :checkout)
    end
  end

  defp wait_for_connection(cluster) do
    case GenServer.call(cluster, :checkout) do
      {:ok, _} -> :ok
      _ -> wait_for_connection(cluster)
    end
  end
end
