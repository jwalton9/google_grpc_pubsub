defmodule Google.Pubsub.Testing do
  import ExUnit.Assertions
  alias Google.Pubsub.{Message, Testing}
  alias Google.Pubsub.V1.PubsubMessage

  def publish(subscription_id, messages) do
    send(self(), {:messages_published, subscription_id, messages})
  end

  def publish(subscription_id, mod, messages) do
    ack_ids =
      messages
      |> mod.handle_messages()
      |> Enum.map(fn %Message{ack_id: ack_id} -> ack_id end)

    Testing.Client.acknowledge(subscription_id, ack_ids)
  end

  def assert_topic_created(topic_id) do
    assert_receive({:topic_created, ^topic_id})
  end

  defmacro assert_messages_published(topic_id, messages, timeout \\ nil) do
    quote do
      assert_receive(
        {:messages_published, unquote(topic_id), messages},
        unquote(timeout),
        "Expected messages to be published, but none were"
      )

      published_messages =
        messages
        |> Enum.map(fn %PubsubMessage{data: data} ->
          case Poison.decode(data) do
            {:ok, data} -> data
            _ -> data
          end
        end)

      assert match?(published_messages, unquote(messages))
    end
  end

  def assert_subscription_created(topic_id, subscription_id) do
    assert_receive({:create_subscription, ^topic_id, ^subscription_id})
  end

  def assert_acknowledged_messages(subscription_id, ack_ids) do
    assert_receive({:acknowledged_messages, ^subscription_id, ^ack_ids})
  end
end
