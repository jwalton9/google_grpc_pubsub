defmodule Pubsub do
  @moduledoc """
  Documentation for `Pubsub`.
  """
  use Application

  @impl true
  def start(_type, _opts) do
    poolboy_config = [
      name: {:local, :grpc_client_pool},
      worker_module: Pubsub.Client,
      size: 3
    ]

    children = [
      :poolboy.child_spec(:grpc_client_pool, poolboy_config)
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Pubsub.Supervisor)
  end
end
