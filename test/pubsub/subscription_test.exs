defmodule Google.Pubsub.SubscriptionTest do
  use ExUnit.Case
  import Google.Pubsub.Testing

  alias Google.Pubsub.{Subscription, Message}

  describe "create/1" do
    test "should create a subscription project and subscription provided" do
      subscription = %Google.Pubsub.V1.Subscription{
        topic: "projects/test/topics/topic",
        name: "projects/test/subscriptions/subscription"
      }

      assert {:ok, ^subscription} =
               Subscription.create(project: "test", subscription: "subscription", topic: "topic")

      assert_subscription_created(
        "projects/test/topics/topic",
        "projects/test/subscriptions/subscription"
      )
    end
  end

  describe "id/1" do
    test "returns a properly formatted id string" do
      assert Subscription.id(project: "my-project", subscription: "test-subscription") ==
               "projects/my-project/subscriptions/test-subscription"
    end

    test "raises an error if subscription missing" do
      catch_error(Subscription.id(project: "my-project"))
    end

    test "raises an error if project missing" do
      catch_error(Subscription.id(subscription: "my-subscription"))
    end
  end

  describe "pull/2" do
    test "defaults to 10 max messages" do
      publish("projects/test/subscriptions/subscription", [%Message{data: "foo"}])

      assert {:ok, [%Message{data: "foo"}]} =
               Subscription.pull(%Google.Pubsub.V1.Subscription{
                 name: "projects/test/subscriptions/subscription"
               })
    end

    test "when setting max_messages" do
      publish("projects/test/subscriptions/subscription", [
        %Message{data: "foo"},
        %Message{data: "bar"}
      ])

      assert {:ok, [%Message{data: "foo"}]} =
               Subscription.pull(
                 %Google.Pubsub.V1.Subscription{
                   name: "projects/test/subscriptions/subscription"
                 },
                 max_messages: 1
               )
    end

    test "returns an empty list if no messages on subscription" do
      publish("projects/test/subscriptions/subscription", [])

      assert {:ok, []} =
               Subscription.pull(%Google.Pubsub.V1.Subscription{
                 name: "projects/test/subscriptions/subscription"
               })
    end
  end

  describe "acknowledge/2" do
    test "returns ok if messages are acked" do
      assert :ok =
               Subscription.acknowledge(
                 %Google.Pubsub.V1.Subscription{
                   name: "projects/test/subscriptions/subscription"
                 },
                 [
                   %Message{ack_id: "projects/test/subscriptions/subscription:1", data: "data"},
                   %Message{ack_id: "projects/test/subscriptions/subscription:2", data: "data"}
                 ]
               )

      assert_acknowledged_messages("projects/test/subscriptions/subscription", [
        "projects/test/subscriptions/subscription:1",
        "projects/test/subscriptions/subscription:2"
      ])
    end
  end
end
