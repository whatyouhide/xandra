defmodule Xandra.Cluster.TokenRingTest do
  use ExUnit.Case, async: true

  alias Xandra.Cluster.Host
  alias Xandra.Cluster.TokenRing

  defp host(address, tokens) do
    %Host{address: address, port: 9042, tokens: MapSet.new(tokens)}
  end

  test "build/1 returns nil with no hosts or no tokens" do
    assert TokenRing.build([]) == nil
    assert TokenRing.build([host({127, 0, 0, 1}, [])]) == nil
  end

  test "build/1 returns nil for non-numeric tokens (unsupported partitioners)" do
    # ByteOrderedPartitioner tokens are hex-ish strings, not integers.
    assert TokenRing.build([host({127, 0, 0, 1}, ["0016a765"])]) == nil
  end

  test "owner_peername/2 finds the owner of a token" do
    ring =
      TokenRing.build([
        host({127, 0, 0, 1}, ["-100", "100"]),
        host({127, 0, 0, 2}, ["-200", "0", "200"])
      ])

    # The owner of a token is the host owning the smallest ring token that is
    # >= the token.
    assert TokenRing.owner_peername(ring, -200) == {{127, 0, 0, 2}, 9042}
    assert TokenRing.owner_peername(ring, -150) == {{127, 0, 0, 1}, 9042}
    assert TokenRing.owner_peername(ring, -100) == {{127, 0, 0, 1}, 9042}
    assert TokenRing.owner_peername(ring, -99) == {{127, 0, 0, 2}, 9042}
    assert TokenRing.owner_peername(ring, 0) == {{127, 0, 0, 2}, 9042}
    assert TokenRing.owner_peername(ring, 1) == {{127, 0, 0, 1}, 9042}
    assert TokenRing.owner_peername(ring, 100) == {{127, 0, 0, 1}, 9042}
    assert TokenRing.owner_peername(ring, 150) == {{127, 0, 0, 2}, 9042}
    assert TokenRing.owner_peername(ring, 200) == {{127, 0, 0, 2}, 9042}

    # Tokens greater than the greatest ring token wrap around to the owner of
    # the smallest ring token.
    assert TokenRing.owner_peername(ring, 201) == {{127, 0, 0, 2}, 9042}
    assert TokenRing.owner_peername(ring, 9_223_372_036_854_775_807) == {{127, 0, 0, 2}, 9042}
  end

  test "owner_peername/2 with a single-host ring" do
    ring = TokenRing.build([host({127, 0, 0, 1}, ["42"])])

    assert TokenRing.owner_peername(ring, -9_223_372_036_854_775_808) == {{127, 0, 0, 1}, 9042}
    assert TokenRing.owner_peername(ring, 42) == {{127, 0, 0, 1}, 9042}
    assert TokenRing.owner_peername(ring, 43) == {{127, 0, 0, 1}, 9042}
  end
end
