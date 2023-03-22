defmodule Xandra.Cluster.HostTest do
  use ExUnit.Case, async: true

  alias Xandra.Cluster.Host

  describe "format_address/1" do
    test "formats an IPv4 address" do
      host = %Host{address: {127, 0, 0, 1}, port: 9042}
      assert Host.format_address(host) == "127.0.0.1:9042"
    end

    test "formats an IPv6 address" do
      host = %Host{address: {0, 0, 0, 0, 0, 0, 0, 1}, port: 9042}
      assert Host.format_address(host) == "::1:9042"
    end

    test "formats a hostname" do
      host = %Host{address: 'cassandra.example.net', port: 9042}
      assert Host.format_address(host) == "cassandra.example.net:9042"
    end
  end
end
