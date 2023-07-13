defmodule Xandra.OptionsValidators do
  # Validator functions used by nimble_options schema definitions all throughout Xandra.
  @moduledoc false

  @spec validate_module(term(), String.t()) :: {:ok, module()} | {:error, String.t()}
  def validate_module(value, name) when is_binary(name) do
    cond do
      is_atom(value) and Code.ensure_loaded?(value) ->
        {:ok, value}

      is_atom(value) ->
        {:error, "#{name} module #{inspect(value)} is not loaded"}

      true ->
        {:error, "expected #{name} module to be a module, got: #{inspect(value)}"}
    end
  end

  @spec validate_authentication(term()) :: {:ok, module()} | {:error, String.t()}
  def validate_authentication({module, keyword} = value)
      when is_atom(module) and is_list(keyword) do
    with {:ok, _module} <- validate_module(module, "authentication"), do: {:ok, value}
  end

  def validate_authentication(other) do
    {:error, "expected :authentication to be a {module, options} tuple, got: #{inspect(other)}"}
  end

  @spec validate_node(term()) :: {:ok, {charlist(), integer()}} | {:error, String.t()}
  def validate_node(value) when is_binary(value) do
    IO.puts("validate_node binary: #{inspect(value)}")
    case String.split(value, ":", parts: 2) do
      [address, port] ->
        case Integer.parse(port) do
          {port, ""} -> {:ok, {String.to_charlist(address), port}}
          _ -> {:error, "invalid node: #{inspect(value)}"}
        end

      [address] ->
        {:ok, {String.to_charlist(address), 9042}}
    end
  end

  def validate_node(%Xandra.Cluster.Host{address: address, port: port}) when is_tuple(address) do
    IO.puts("validate_node tuple address: #{inspect(address)}, port: #{port}")
    case :inet.ntoa(address) do
      {:error, :einval} ->
        {:error,
         "expected valid address, got: tuple address: #{inspect(address)} and port: #{inspect(port)}, with error: :einval"}

      valid_address ->
        {:ok, {valid_address, port}}
    end
  end

  def validate_node(%Xandra.Cluster.Host{address: address, port: port}) when is_list(address) do
    IO.puts("validate_node list address: #{inspect(address)}, port: #{port}")
    case :inet.parse_address(address) do
      {:ok, _} ->
        {:ok, {address, port}}

      error ->
        {:error,
         "expected valid address, got: list address: #{inspect(address)} and port: #{inspect(port)}, with error: #{inspect(error)}"}
    end
  end

  def validate_node(other) do
    {:error, "expected node to be a string or a {ip, port} tuple, got: #{inspect(other)}"}
  end

  @spec validate_binary(term(), atom()) :: {:ok, binary()} | {:error, String.t()}
  def validate_binary(value, key) when is_atom(key) do
    if is_binary(value) do
      {:ok, value}
    else
      {:error, "expected #{inspect(key)} to be a binary, got: #{inspect(value)}"}
    end
  end

  @spec validate_custom_payload(term()) :: {:ok, Xandra.custom_payload()} | {:error, String.t()}
  def validate_custom_payload(term) do
    if is_map(term) and Enum.all?(term, fn {key, val} -> is_binary(key) and is_binary(val) end) do
      {:ok, term}
    else
      {:error, "expected custom payload to be a map of string to binary, got: #{inspect(term)}"}
    end
  end
end
