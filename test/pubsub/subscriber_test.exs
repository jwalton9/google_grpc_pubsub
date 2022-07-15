defmodule Google.Pubsub.TestSubscriber do
  use Google.Pubsub.Subscriber

  @impl true
  def handle_messages(messages) do
    send(self(), {:received_messages, messages})

    messages
  end
end

defmodule Google.Pubsub.SubscriberTest do
  use ExUnit.Case
  use Google.Pubsub.Testing

  alias Google.Pubsub.Message

  test "calls handles_messages whenever messages are published" do
    publish("projects/my-project/subscriptions/hello", Google.Pubsub.TestSubscriber, [
      %Message{ack_id: "0", data: "Hello world"}
    ])

    assert_receive(
      {:received_messages,
       [
         %Google.Pubsub.Message{
           ack_id: "0",
           data: "Hello world"
         }
       ]}
    )

    assert_acknowledged_messages("projects/my-project/subscriptions/hello", [
      "0"
    ])
  end
end
