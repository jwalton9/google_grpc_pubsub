defmodule Google.Pubsub do
  @moduledoc """
  Pubsub is split into two parts:
  - `Pubsub.Topic` - Publish messages to a topic
  - `Pubsub.Subscription` - Pull messages from a subscription
  - `Pubsub.Subscriber` - Starts a stream of pubsub messages and passes them to the provided handler.
  """

  use Application

  def start() do
    Supervisor.start_child(__MODULE__, pool_child_spec())
  end

  def start(_type, _opts) do
    start_pool = Application.get_env(:google_grpc_pubsub, :start_pool, true)

    children = if start_pool, do: [pool_child_spec()], else: []

    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
  end

  defp pool_name() do
    :grpc_connection_pool
  end

  defp pool_config() do
    [
      name: {:local, pool_name()},
      worker_module: Google.Pubsub.Connection,
      size: Application.get_env(:google_grpc_pubsub, :pool_size, 10)
    ]
  end

  defp pool_child_spec() do
    :poolboy.child_spec(:grpc_connection_pool, pool_config())
  end
end
