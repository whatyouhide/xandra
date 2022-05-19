defmodule Xandra.FrameTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  use Bitwise, only_operators: true

  alias Xandra.Frame

  @max_v5_payload_size_in_bytes 128 * 1024 - 1

  describe "max_supported_protocol/0" do
    test "returns a protocol version" do
      assert Frame.max_supported_protocol() == :v4
    end
  end

  describe "supported_protocols/0" do
    test "returns a list of supported protocols" do
      assert Enum.sort(Frame.supported_protocols()) == [:v3, :v4, :v5]
    end
  end

  property "encode_v4/2 and decode_from_binary/2 are circular" do
    compressor = nil

    check all protocol_version <- member_of([:v3, :v4]),
              kind <- kind_generator(),
              tracing? <- boolean(),
              body <- binary(),
              stream_id <- map(integer(), &abs/1) do
      protocol_module = Frame.protocol_version_to_module(protocol_version)

      frame = Frame.new(kind, compressor: compressor, tracing: tracing?)

      frame = %Frame{
        frame
        | body: body,
          stream_id: stream_id,
          protocol_version: protocol_version
      }

      assert {redecoded_frame, _rest = ""} =
               frame
               |> Frame.encode_v4(protocol_module)
               |> IO.iodata_to_binary()
               |> Frame.decode_from_binary(compressor)

      assert frame == redecoded_frame
    end
  end

  describe "encoding and decoding with native protocol v5" do
    property "with self-contained random contents, without compression" do
      check all inner_payload <- iodata() do
        encoded = inner_payload |> Frame.encode_v5_wrappers() |> IO.iodata_to_binary()
        assert {:ok, redecoded} = Frame.decode_v5_wrapper(&fetch_bytes_from_binary/2, encoded)
        assert IO.iodata_to_binary(inner_payload) == IO.iodata_to_binary(redecoded)
      end
    end

    property "with a big inner content that spans multiple frames (not self contained), without compression" do
      size_range = (@max_v5_payload_size_in_bytes + 1)..(@max_v5_payload_size_in_bytes * 3)

      check all size <- integer(size_range),
                max_runs: 3 do
        big_payload = :binary.copy(<<0>>, size)

        encoded = big_payload |> Frame.encode_v5_wrappers() |> IO.iodata_to_binary()
        assert {:ok, redecoded} = Frame.decode_v5_wrapper(&fetch_bytes_from_binary/2, encoded)

        assert big_payload == IO.iodata_to_binary(redecoded)
      end
    end

    property "with v4- frames inside the v5+ wrappers" do
      check all kind <- kind_generator(),
                tracing? <- boolean(),
                body <- binary(),
                stream_id <- map(integer(), &abs/1) do
        protocol_module = Frame.protocol_version_to_module(:v5)

        frame = Frame.new(kind, compressor: nil, tracing: tracing?)

        frame = %Frame{
          frame
          | body: body,
            stream_id: stream_id,
            protocol_version: :v5
        }

        encoded =
          frame
          |> Frame.encode(protocol_module)
          |> IO.iodata_to_binary()

        assert {:ok, redecoded_frame} =
                 Frame.decode_v5(&fetch_bytes_from_binary/2, encoded, _compressor = nil)

        assert redecoded_frame == frame
      end
    end
  end

  defp kind_generator do
    member_of([
      :startup,
      :options,
      :query,
      :prepare,
      :execute,
      :register,
      :batch,
      :auth_response,
      :error,
      :ready,
      :authenticate,
      :supported,
      :result,
      :event,
      :auth_success
    ])
  end

  defp fetch_bytes_from_binary(binary, size) do
    case binary do
      <<chunk::size(size)-binary, rest::binary>> -> {:ok, chunk, rest}
      _other -> {:error, :not_enough_bytes}
    end
  end
end
