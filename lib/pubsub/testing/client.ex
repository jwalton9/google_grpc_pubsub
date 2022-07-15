defmodule Google.Pubsub.Testing.Client do
  import ExUnit.Assertions

  alias Google.Pubsub.V1.{
    Topic,
    PublishResponse,
    Subscription,
    PullResponse,
    ReceivedMessage
  }

  def create_topic(id) do
    send(self(), {:topic_created, id})
    {:ok, Topic.new(name: id)}
  end

  def get_topic(id) do
    {:ok, Topic.new(name: id)}
  end

  def publish(topic_id, messages) do
    send(self(), {:messages_published, topic_id, messages})
    {:ok, PublishResponse.new()}
  end

  def create_subscription(topic_id, subscription_id) do
    send(self(), {:create_subscription, topic_id, subscription_id})

    {:ok,
     Subscription.new(
       topic: topic_id,
       name: subscription_id
     )}
  end

  def get_subscription(subscription_id) do
    {:ok, Subscription.new(name: subscription_id)}
  end

  def delete_subscription(_subscription_id) do
    {:ok, Google.Protobuf.Empty.new()}
  end

  def pull(subscription_id, max_messages \\ 10) do
    assert_receive({:messages_published, ^subscription_id, messages})

    received_messages =
      messages
      |> Enum.take(max_messages)
      |> Enum.map(fn message ->
        ReceivedMessage.new(ack_id: to_string(:rand.uniform()), message: message)
      end)

    {:ok, PullResponse.new(received_messages: received_messages)}
  end

  def acknowledge(subscription_id, ack_ids) do
    send(self(), {:acknowledged_messages, subscription_id, ack_ids})
    {:ok, Google.Protobuf.Empty.new()}
  end
end
