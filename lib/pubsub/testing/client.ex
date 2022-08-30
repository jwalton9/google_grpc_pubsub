defmodule Google.Pubsub.Testing.Client do
  import ExUnit.Assertions

  alias Google.Pubsub.Message

  alias Google.Pubsub.V1.{
    Topic,
    Subscription,
    ReceivedMessage,
    PubsubMessage,
    PublishResponse,
    PullResponse
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
      |> Enum.map(fn %Message{
                       ack_id: ack_id,
                       data: data,
                       attributes: attributes,
                       delivery_attempt: delivery_attempt,
                       publish_time: publish_time
                     } ->
        IO.inspect(publish_time, label: "HSASJFOIASF")

        ReceivedMessage.new(
          ack_id: ack_id || to_string(:rand.uniform()),
          delivery_attempt: delivery_attempt,
          message: %PubsubMessage{
            data: data,
            attributes: attributes,
            publish_time: %Google.Protobuf.Timestamp{
              seconds: if(publish_time, do: DateTime.to_unix(publish_time), else: 0),
              nanos: 0
            }
          }
        )
      end)

    {:ok, PullResponse.new(received_messages: received_messages)}
  end

  def acknowledge(subscription_id, ack_ids) do
    send(self(), {:acknowledged_messages, subscription_id, ack_ids})
    {:ok, Google.Protobuf.Empty.new()}
  end
end
