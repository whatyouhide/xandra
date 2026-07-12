defmodule Xandra.Connection.UtilsTest do
  use ExUnit.Case, async: true

  alias Xandra.Connection.Utils
  alias Xandra.Frame
  alias Xandra.Protocol.V4
  alias Xandra.Transport

  defmodule ChallengeAuthenticator do
    @behaviour Xandra.Authenticator

    @impl true
    def response_body(options) do
      Keyword.fetch!(options, :initial_response)
    end

    @impl true
    def challenge_response_body(challenge, options) do
      send(Keyword.fetch!(options, :test_pid), {:challenge_received, challenge})
      Keyword.fetch!(options, :challenge_response)
    end
  end

  defmodule NoChallengeAuthenticator do
    @behaviour Xandra.Authenticator

    @impl true
    def response_body(_options), do: "SigV4\0\0"
  end

  @requested_options %{"CQL_VERSION" => "3.4.5"}

  describe "startup_connection/5 with an AUTH_CHALLENGE flow" do
    test "completes the challenge round-trip and returns :ok" do
      test_pid = self()

      port =
        start_fake_server(fn socket ->
          assert %Frame{kind: :startup} = server_recv_frame(socket)
          server_send_frame(socket, :authenticate, <<>>)

          assert %Frame{kind: :auth_response, body: <<len::32, token::binary-size(len)>>} =
                   server_recv_frame(socket)

          send(test_pid, {:server, :initial_response, token})

          nonce = "the-nonce"
          server_send_frame(socket, :auth_challenge, <<byte_size(nonce)::32, nonce::binary>>)

          assert %Frame{kind: :auth_response, body: <<len2::32, token2::binary-size(len2)>>} =
                   server_recv_frame(socket)

          send(test_pid, {:server, :challenge_response, token2})

          server_send_frame(socket, :auth_success, <<>>)
        end)

      transport = connect(port)

      authentication =
        {ChallengeAuthenticator,
         initial_response: "SigV4\0\0", challenge_response: "signed-nonce", test_pid: test_pid}

      assert Utils.startup_connection(transport, @requested_options, V4, nil,
               authentication: authentication
             ) == :ok

      assert_receive {:server, :initial_response, "SigV4\0\0"}
      assert_receive {:challenge_received, "the-nonce"}
      assert_receive {:server, :challenge_response, "signed-nonce"}
    end

    test "raises if the authenticator doesn't implement challenge_response_body/2" do
      port =
        start_fake_server(fn socket ->
          assert %Frame{kind: :startup} = server_recv_frame(socket)
          server_send_frame(socket, :authenticate, <<>>)
          assert %Frame{kind: :auth_response} = server_recv_frame(socket)
          nonce = "the-nonce"
          server_send_frame(socket, :auth_challenge, <<byte_size(nonce)::32, nonce::binary>>)
        end)

      transport = connect(port)

      assert_raise RuntimeError, ~r/does not implement the optional/, fn ->
        Utils.startup_connection(transport, @requested_options, V4, nil,
          authentication: {NoChallengeAuthenticator, []}
        )
      end
    end
  end

  defp connect(port) do
    transport = %Transport{module: :gen_tcp, options: [:binary, packet: :raw, active: false]}
    {:ok, transport} = Transport.connect(transport, ~c"localhost", port, 5000)
    transport
  end

  defp start_fake_server(handler) do
    {:ok, listen_socket} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true])

    {:ok, port} = :inet.port(listen_socket)

    spawn(fn ->
      {:ok, socket} = :gen_tcp.accept(listen_socket, 5000)
      handler.(socket)
    end)

    port
  end

  defp server_recv_frame(socket) do
    {:ok, header} = :gen_tcp.recv(socket, Frame.header_length(), 5000)

    body =
      case Frame.body_length(header) do
        0 -> <<>>
        length -> with {:ok, body} <- :gen_tcp.recv(socket, length, 5000), do: body
      end

    Frame.decode(header, body)
  end

  defp server_send_frame(socket, kind, body) do
    payload =
      Frame.new(kind, [])
      |> Map.put(:body, body)
      |> Frame.encode_v4(V4)

    :ok = :gen_tcp.send(socket, payload)
  end
end
