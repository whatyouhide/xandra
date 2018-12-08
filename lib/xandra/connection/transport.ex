import Kernel, except: [send: 2]

defmodule Xandra.Connection.Transport do
  @type t() :: {:gen_tcp, :gen_tcp.socket()}

  def connect(address, port, options, timeout) do
    with {:ok, socket} <- :gen_tcp.connect(address, port, options, timeout) do
      {:ok, {:gen_tcp, socket}}
    end
  end

  def close({mod, socket}) do
    mod.close(socket)
  end

  def send({mod, socket}, payload) do
    mod.send(socket, payload)
  end

  def recv({mod, socket}, payload) do
    mod.recv(socket, payload)
  end

  def peername({:gen_tcp, socket}) do
    :inet.peername(socket)
  end

  def setopts({:gen_tcp, socket}, options) do
    :inet.setopts(socket, options)
  end
end
