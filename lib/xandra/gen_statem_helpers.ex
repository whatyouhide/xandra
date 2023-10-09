defmodule Xandra.GenStatemHelpers do
  @moduledoc false

  # We use :gen_statem in some places, but name registration for it is tricky for example.
  # This module provides a few helpers to mitigate that.

  @spec split_opts(keyword()) :: {keyword(), keyword()}
  def split_opts(opts) when is_list(opts) do
    {gen_statem_opts, other_opts} = Keyword.split(opts, [:debug, :hibernate_after, :spawn_opt])
    gen_statem_opts = Keyword.merge(gen_statem_opts, Keyword.take(other_opts, [:name]))
    {gen_statem_opts, other_opts}
  end

  @spec start_link_with_name_registration(module(), term(), keyword()) :: :gen_statem.start_ret()
  def start_link_with_name_registration(module, arg, gen_statem_opts_with_name)
      when is_atom(module) and is_list(gen_statem_opts_with_name) do
    case Keyword.fetch(gen_statem_opts_with_name, :name) do
      :error ->
        :gen_statem.start_link(module, arg, gen_statem_opts_with_name)

      {:ok, atom} when is_atom(atom) ->
        gen_statem_opts = Keyword.delete(gen_statem_opts_with_name, :name)
        :gen_statem.start_link({:local, atom}, module, arg, gen_statem_opts)

      {:ok, {:global, _term} = tuple} ->
        gen_statem_opts = Keyword.delete(gen_statem_opts_with_name, :name)
        :gen_statem.start_link(tuple, module, arg, gen_statem_opts)

      {:ok, {:via, via_module, _term} = tuple} when is_atom(via_module) ->
        gen_statem_opts = Keyword.delete(gen_statem_opts_with_name, :name)
        :gen_statem.start_link(tuple, module, arg, gen_statem_opts)

      {:ok, other} ->
        raise ArgumentError, """
        expected :name option to be one of the following:

          * nil
          * atom
          * {:global, term}
          * {:via, module, term}

        Instead, got: #{inspect(other)}\
        """
    end
  end
end
