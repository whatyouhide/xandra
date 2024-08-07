defmodule Xandra.FrameTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Xandra.Frame
  alias Xandra.TestHelper.LZ4Compressor

  @max_v5_payload_size_in_bytes 128 * 1024 - 1

  describe "max_supported_protocol/0" do
    test "returns a protocol version" do
      assert Frame.max_supported_protocol() == :v5
    end
  end

  describe "previous_protocol/1" do
    test "returns the previous version of each protocol (for downgrading)" do
      assert Frame.previous_protocol(:v5) == :v4
      assert Frame.previous_protocol(:v4) == :v3
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
        encoded =
          inner_payload |> Frame.encode_v5_wrappers(_compressor = nil) |> IO.iodata_to_binary()

        assert {:ok, redecoded, _rest = ""} =
                 Frame.decode_v5_wrapper(
                   &Frame.fetch_bytes_from_binary/2,
                   encoded,
                   _compressor = nil
                 )

        assert IO.iodata_to_binary(inner_payload) == IO.iodata_to_binary(redecoded)
      end
    end

    @tag :compression
    property "with self-contained random contents, with compression" do
      check all inner_payload <- iodata(),
                IO.iodata_length(inner_payload) > 0,
                initial_size: 5 do
        encoded =
          inner_payload
          |> Frame.encode_v5_wrappers(_compressor = LZ4Compressor)
          |> IO.iodata_to_binary()

        assert {:ok, redecoded, _rest = ""} =
                 Frame.decode_v5_wrapper(
                   &Frame.fetch_bytes_from_binary/2,
                   encoded,
                   _compressor = LZ4Compressor
                 )

        assert IO.iodata_to_binary(inner_payload) == IO.iodata_to_binary(redecoded)
      end
    end

    @tag :compression
    test "with an empty uncompressed payload and with compression, uses the compressed payload" do
      encoded =
        ""
        |> Frame.encode_v5_wrappers(_compressor = LZ4Compressor)
        |> IO.iodata_to_binary()

      assert {:ok, redecoded, _rest = ""} =
               Frame.decode_v5_wrapper(
                 &Frame.fetch_bytes_from_binary/2,
                 encoded,
                 _compressor = LZ4Compressor
               )

      assert IO.iodata_to_binary(redecoded) == <<0>>
    end

    property "with a big inner content that spans multiple frames (not self contained), without compression" do
      size_range = (@max_v5_payload_size_in_bytes + 1)..(@max_v5_payload_size_in_bytes * 3)

      check all size <- integer(size_range),
                max_runs: 5 do
        # We put the v4- header here because we use the length it contains in order to decide when
        # to stop parsing.
        inner_frame = <<0::5-unit(8), size::32>> <> :binary.copy(<<0>>, size)

        encoded =
          inner_frame |> Frame.encode_v5_wrappers(_compressor = nil) |> IO.iodata_to_binary()

        assert {:ok, redecoded, _rest = ""} =
                 Frame.decode_v5_wrapper(
                   &Frame.fetch_bytes_from_binary/2,
                   encoded,
                   _compressor = nil
                 )

        assert inner_frame == IO.iodata_to_binary(redecoded)
      end
    end

    property "with v4- frames inside the v5+ wrappers" do
      check all kind <- kind_generator(),
                tracing? <- boolean(),
                body <- binary(),
                stream_id <- integer(1..1000) do
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

        assert {:ok, [redecoded_frame], _rest = ""} =
                 Frame.decode_v5(&Frame.fetch_bytes_from_binary/2, encoded, _compressor = nil)

        assert redecoded_frame == frame
      end
    end
  end

  describe "decode_v5_wrapper/3" do
    test "with mismatching CRC for the header (without compression)" do
      <<header_data::3-bytes, _header_crc::3-bytes, rest::binary>> =
        ""
        |> Frame.encode_v5_wrappers(_compressor = nil)
        |> IO.iodata_to_binary()

      malformed_crc = <<1, 2, 3>>
      malformed_encoded = <<header_data::binary, malformed_crc::binary, rest::binary>>

      assert_raise RuntimeError, "mismatching CRC24 for header", fn ->
        Frame.decode_v5_wrapper(
          &Frame.fetch_bytes_from_binary/2,
          malformed_encoded,
          _compressor = nil
        )
      end
    end

    test "with mismatching CRC for the payload (without compression)" do
      payload = :crypto.strong_rand_bytes(Enum.random(0..10))
      payload_size = byte_size(payload)

      <<header::6-bytes, ^payload::size(payload_size)-binary, _payload_crc::4-bytes>> =
        payload
        |> Frame.encode_v5_wrappers(_compressor = nil)
        |> IO.iodata_to_binary()

      malformed_crc = <<1, 2, 3, 4>>
      malformed_encoded = <<header::binary, payload::binary, malformed_crc::binary>>

      assert_raise RuntimeError, "mismatching CRC32 for payload", fn ->
        Frame.decode_v5_wrapper(
          &Frame.fetch_bytes_from_binary/2,
          malformed_encoded,
          _compressor = nil
        )
      end
    end

    @tag :compression
    test "with mismatching CRC for the header (with compression)" do
      <<header_data::5-bytes, _header_crc::3-bytes, rest::binary>> =
        ""
        |> Frame.encode_v5_wrappers(_compressor = LZ4Compressor)
        |> IO.iodata_to_binary()

      malformed_crc = <<1, 2, 3>>
      malformed_encoded = <<header_data::binary, malformed_crc::binary, rest::binary>>

      assert_raise RuntimeError, "mismatching CRC24 for header", fn ->
        Frame.decode_v5_wrapper(
          &Frame.fetch_bytes_from_binary/2,
          malformed_encoded,
          _compressor = LZ4Compressor
        )
      end
    end

    @tag :compression
    test "with mismatching CRC for the payload (with compression)" do
      payload = :crypto.strong_rand_bytes(Enum.random(0..10))

      <<_length::4-bytes, compressed_payload::binary>> =
        payload |> LZ4Compressor.compress() |> IO.iodata_to_binary()

      compressed_payload_size = byte_size(compressed_payload)

      <<header::8-bytes, ^compressed_payload::size(compressed_payload_size)-bytes,
        _payload_crc::4-bytes>> =
        payload
        |> Frame.encode_v5_wrappers(_compressor = LZ4Compressor)
        |> IO.iodata_to_binary()

      malformed_crc = <<1, 2, 3, 4>>
      malformed_encoded = <<header::binary, compressed_payload::binary, malformed_crc::binary>>

      assert_raise RuntimeError, "mismatching CRC32 for payload", fn ->
        Frame.decode_v5_wrapper(
          &Frame.fetch_bytes_from_binary/2,
          malformed_encoded,
          _compressor = LZ4Compressor
        )
      end
    end

    test "bubbles up errors returned by the \"fetch_bytes_fun\"" do
      faulty_fetch_bytes_fun = fn _fetch_state, _byte_count -> {:error, :some_error} end

      assert Frame.decode_v5_wrapper(faulty_fetch_bytes_fun, _payload = "", _compressor = nil) ==
               {:error, :some_error}
    end
  end

  describe "decode/5" do
    # Regression for: https://issues.apache.org/jira/browse/CASSANDRA-19753
    @tag :regression
    test "can decode multiple envelopes in a single frame" do
      payload =
        <<144, 0, 2, 138, 218, 155, 133, 0, 0, 2, 8, 0, 0, 0, 63, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0,
          1, 0, 6, 115, 121, 115, 116, 101, 109, 0, 5, 108, 111, 99, 97, 108, 0, 12, 99, 108, 117,
          115, 116, 101, 114, 95, 110, 97, 109, 101, 0, 13, 0, 0, 0, 1, 0, 0, 0, 12, 116, 101,
          115, 116, 95, 99, 108, 117, 115, 116, 101, 114, 133, 0, 0, 1, 8, 0, 0, 0, 63, 0, 0, 0,
          2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 115, 121, 115, 116, 101, 109, 0, 5, 108, 111, 99, 97,
          108, 0, 12, 99, 108, 117, 115, 116, 101, 114, 95, 110, 97, 109, 101, 0, 13, 0, 0, 0, 1,
          0, 0, 0, 12, 116, 101, 115, 116, 95, 99, 108, 117, 115, 116, 101, 114, 40, 65, 100, 21>>

      assert {:ok, [%Frame{stream_id: 2}, %Frame{stream_id: 1}], _rest = ""} =
               Frame.decode(
                 Xandra.Protocol.V5,
                 &Frame.fetch_bytes_from_binary/2,
                 payload,
                 _compressor = nil,
                 _rest_fun = & &1
               )
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
end
