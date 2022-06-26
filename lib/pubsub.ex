defmodule Pubsub do
  @moduledoc """
  Pubsub is split into two parts:
  - `Pubsub.Subscriber` - Starts a stream of pubsub messages and passes them to the provided handler.
  - `Pubsub.Publisher` - Publishes messages to a pubsub topic.
  """
  use Application

  alias Google.Pubsub.V1.{
    Subscription,
    Topic
  }

  @impl true
  def start(_type, _opts) do
    children = [
      {Task.Supervisor, name: Pubsub.TaskSupervisor, max_children: 50},
      {Pubsub.Client, []}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
  end

  @spec publish(Topic.t(), String.t() | map() | [String.t() | map()]) ::
          :ok | {:error, any()}
  def publish(topic, data) do
    Pubsub.Publisher.publish(topic, data)
  end

  @spec pull(Subscription.t(), Keyword.t()) :: {:ok, [Pubsub.Message.t()]} | {:error, any()}
  def pull(subscription, opts \\ []) do
    Pubsub.Subscriber.pull(subscription, opts)
  end

  @spec acknowledge([Pubsub.Message.t()], Subscription.t()) ::
          :ok | {:error, any()}
  def acknowledge(messages, subscription) do
    Pubsub.Subscriber.acknowledge(subscription, messages)
  end
end
