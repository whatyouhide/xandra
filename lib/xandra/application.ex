defmodule Xandra.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {Task.Supervisor, name: Xandra.TaskSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
