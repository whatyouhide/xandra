defmodule Xandra.OptionsValidators do
  # Validator functions used by nimble_options schema definitions all throughout Xandra.
  @moduledoc false

  @doc false
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

  @doc false
  def validate_authentication({module, keyword} = value)
      when is_atom(module) and is_list(keyword) do
    with {:ok, _module} <- validate_module(module, "authentication"), do: {:ok, value}
  end

  def validate_authentication(other) do
    {:error, "expected :authentication to be a {module, options} tuple, got: #{inspect(other)}"}
  end

  @doc false
  def validate_ip({a, b, c, d} = ip)
      when is_integer(a) and is_integer(b) and is_integer(c) and is_integer(d) do
    {:ok, ip}
  end

  def validate_ip(other) do
    {:error, "invalid IP address: #{inspect(other)}"}
  end
end
