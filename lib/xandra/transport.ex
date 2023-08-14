defmodule Xandra.Transport do
  @moduledoc false

  # Internal module to abstract away operations on transport sockets, independent from
  # the transport (TCP or SSL).

  @type options() :: [{atom(), term()} | atom()]
  @type error_reason() :: :inet.posix() | term()

  @type t() :: %__MODULE__{
          module: :gen_tcp | :ssl,
          options: options(),
          socket: :gen_tcp.socket() | :ssl.sslsocket() | nil
        }

  @enforce_keys [:module, :options]
  defstruct [:module, :options, :socket]

  @spec connect(t(), :inet.ip_address(), :inet.port_number(), timeout()) ::
          {:ok, t()} | {:error, error_reason}
  def connect(%__MODULE__{socket: nil} = transport, address, port, timeout) do
    with {:ok, socket} <- transport.module.connect(address, port, transport.options, timeout) do
      {:ok, %__MODULE__{transport | socket: socket}}
    end
  end

  @spec send(t(), iodata()) :: :ok | {:error, error_reason}
  def send(%__MODULE__{socket: socket} = transport, data) when not is_nil(socket) do
    transport.module.send(socket, data)
  end

  @spec setopts(t(), options()) :: :ok | {:error, error_reason}
  def setopts(%__MODULE__{} = transport, options) when is_list(options) do
    inet_mod(transport.module).setopts(transport.socket, options)
  end

  @spec address_and_port(t()) ::
          {:ok, {:inet.ip_address(), :inet.port_number()}} | {:error, error_reason}
  def address_and_port(%__MODULE__{socket: socket} = transport) when not is_nil(socket) do
    inet_mod(transport.module).peername(socket)
  end

  @spec close(t()) :: :ok
  def close(%__MODULE__{} = transport) do
    transport.module.close(transport.socket)
  end

  defp inet_mod(:gen_tcp), do: :inet
  defp inet_mod(:ssl), do: :ssl
end
