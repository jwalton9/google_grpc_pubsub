defmodule Pubsub do
  @moduledoc """
  Pubsub is split into two parts:
  - `Pubsub.Topic` - Publish messages to a topic
  - `Pubsub.Subscription` - Pull messages from a subscription
  - `Pubsub.Subscriber` - Starts a stream of pubsub messages and passes them to the provided handler.
  """

  use Supervisor

  def start_link(init) do
    Supervisor.start_link(__MODULE__, init, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    pool_config = [
      name: {:local, :grpc_connection_pool},
      worker_module: Pubsub.Connection,
      size: Application.get_env(:google_grpc_pubsub, :pool_size, 10)
    ]

    children = [
      :poolboy.child_spec(:grpc_connection_pool, pool_config)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
