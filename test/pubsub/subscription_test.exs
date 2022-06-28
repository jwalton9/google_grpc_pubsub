defmodule Google.Pubsub.SubscriptionTest do
  use ExUnit.Case

  alias Google.Pubsub.{Subscription, Message}

  import Mox

  setup :verify_on_exit!

  describe "create/1" do
    test "should create a subscription project and subscription provided" do
      fun = &Google.Pubsub.V1.Subscriber.Stub.create_subscription/3

      subscription = %Google.Pubsub.V1.Subscription{
        topic: "projects/test/topics/topic",
        name: "projects/test/subscriptions/subscription"
      }

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn ^subscription, ^fun ->
        {:ok, subscription}
      end)

      assert {:ok, ^subscription} =
               Subscription.create(project: "test", subscription: "subscription", topic: "topic")
    end

    test "should return an error if the request fails" do
      fun = &Google.Pubsub.V1.Subscriber.Stub.create_subscription/3

      error = %GRPC.RPCError{message: "Subscription already exists", status: 6}

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.Subscription{
                                    name: "projects/test/subscriptions/subscription"
                                  },
                                  ^fun ->
        {:error, error}
      end)

      assert {:error, ^error} =
               Subscription.create(project: "test", subscription: "subscription", topic: "topic")
    end
  end

  describe "get/1" do
    test "should get a subscription when project and subscription provided" do
      fun = &Google.Pubsub.V1.Subscriber.Stub.get_subscription/3

      subscription = %Google.Pubsub.V1.Subscription{
        name: "projects/test/subscriptions/subscription"
      }

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.GetSubscriptionRequest{
                                    subscription: "projects/test/subscriptions/subscription"
                                  },
                                  ^fun ->
        {:ok, subscription}
      end)

      assert {:ok, ^subscription} =
               Subscription.get(project: "test", subscription: "subscription")
    end

    test "should return an error if grpc request fails" do
      fun = &Google.Pubsub.V1.Subscriber.Stub.get_subscription/3

      error = %GRPC.RPCError{message: "Subscription not found", status: 5}

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.GetSubscriptionRequest{
                                    subscription: "projects/test/subscriptions/subscription"
                                  },
                                  ^fun ->
        {:error, error}
      end)

      assert {:error, ^error} = Subscription.get(project: "test", subscription: "subscription")
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
      fun = &Google.Pubsub.V1.Subscriber.Stub.pull/3

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.PullRequest{
                                    max_messages: 10,
                                    subscription: "projects/test/subscriptions/subscription"
                                  },
                                  ^fun ->
        {:ok,
         %Google.Pubsub.V1.PullResponse{
           received_messages: [
             %Google.Pubsub.V1.ReceivedMessage{
               ack_id: "1",
               message: %Google.Pubsub.V1.PubsubMessage{
                 data: "data",
                 attributes: %{
                   "key" => "value"
                 }
               }
             }
           ]
         }}
      end)

      assert {:ok, [%Message{ack_id: "1", data: "data"}]} =
               Subscription.pull(%Google.Pubsub.V1.Subscription{
                 name: "projects/test/subscriptions/subscription"
               })
    end

    test "when setting max_messages" do
      fun = &Google.Pubsub.V1.Subscriber.Stub.pull/3

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.PullRequest{
                                    max_messages: 100,
                                    subscription: "projects/test/subscriptions/subscription"
                                  },
                                  ^fun ->
        {:ok,
         %Google.Pubsub.V1.PullResponse{
           received_messages: [
             %Google.Pubsub.V1.ReceivedMessage{
               ack_id: "1",
               message: %Google.Pubsub.V1.PubsubMessage{
                 data: "data",
                 attributes: %{
                   "key" => "value"
                 }
               }
             }
           ]
         }}
      end)

      assert {:ok, [%Message{ack_id: "1", data: "data"}]} =
               Subscription.pull(
                 %Google.Pubsub.V1.Subscription{
                   name: "projects/test/subscriptions/subscription"
                 },
                 max_messages: 100
               )
    end

    test "returns an empty list if no messages on subscription" do
      fun = &Google.Pubsub.V1.Subscriber.Stub.pull/3

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.PullRequest{
                                    max_messages: 10,
                                    subscription: "projects/test/subscriptions/subscription"
                                  },
                                  ^fun ->
        {:ok,
         %Google.Pubsub.V1.PullResponse{
           received_messages: []
         }}
      end)

      assert {:ok, []} =
               Subscription.pull(%Google.Pubsub.V1.Subscription{
                 name: "projects/test/subscriptions/subscription"
               })
    end

    test "returns an error if the pull fails" do
      fun = &Google.Pubsub.V1.Subscriber.Stub.pull/3

      error = %GRPC.RPCError{status: 5, message: "Subscription not found"}

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.PullRequest{
                                    max_messages: 10,
                                    subscription: "projects/test/subscriptions/subscription"
                                  },
                                  ^fun ->
        {:error, error}
      end)

      assert {:error, ^error} =
               Subscription.pull(%Google.Pubsub.V1.Subscription{
                 name: "projects/test/subscriptions/subscription"
               })
    end
  end

  describe "acknowledge/2" do
    test "returns ok if messages are acked" do
      fun = &Google.Pubsub.V1.Subscriber.Stub.acknowledge/3

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.AcknowledgeRequest{
                                    subscription: "projects/test/subscriptions/subscription",
                                    ack_ids: [
                                      "projects/test/subscriptions/subscription:1",
                                      "projects/test/subscriptions/subscription:2"
                                    ]
                                  },
                                  ^fun ->
        {:ok, %Google.Protobuf.Empty{}}
      end)

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
    end

    test "returns an error if the acknowledge fails" do
      fun = &Google.Pubsub.V1.Subscriber.Stub.acknowledge/3

      error = %GRPC.RPCError{status: 5, message: "Subscription not found"}

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.AcknowledgeRequest{
                                    subscription: "projects/test/subscriptions/subscription",
                                    ack_ids: ["projects/test/subscriptions/subscription:111"]
                                  },
                                  ^fun ->
        {:error, error}
      end)

      assert {:error, ^error} =
               Subscription.acknowledge(
                 %Google.Pubsub.V1.Subscription{
                   name: "projects/test/subscriptions/subscription"
                 },
                 %Message{ack_id: "projects/test/subscriptions/subscription:111"}
               )
    end
  end
end
