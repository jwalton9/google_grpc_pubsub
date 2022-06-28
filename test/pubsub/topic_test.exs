defmodule Google.Pubsub.TopicTest do
  use ExUnit.Case

  alias Google.Pubsub.{Topic, Message}

  import Mox

  setup :verify_on_exit!

  describe "create/1" do
    test "should create a topic project and topic provided" do
      fun = &Google.Pubsub.V1.Publisher.Stub.create_topic/3

      topic = %Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"}

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn ^topic, ^fun ->
        {:ok, topic}
      end)

      assert {:ok, ^topic} = Topic.create(project: "test", topic: "topic")
    end

    test "should return an error if the request fails" do
      fun = &Google.Pubsub.V1.Publisher.Stub.create_topic/3

      error = %GRPC.RPCError{message: "Topic already exists", status: 6}

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"},
                                  ^fun ->
        {:error, error}
      end)

      assert {:error, ^error} = Topic.create(project: "test", topic: "topic")
    end
  end

  describe "get/1" do
    test "should get a topic when project and topic provided" do
      fun = &Google.Pubsub.V1.Publisher.Stub.get_topic/3

      topic = %Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"}

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.GetTopicRequest{
                                    topic: "projects/test/topics/topic"
                                  },
                                  ^fun ->
        {:ok, topic}
      end)

      assert {:ok, ^topic} = Topic.get(project: "test", topic: "topic")
    end

    test "should return an error if grpc request fails" do
      fun = &Google.Pubsub.V1.Publisher.Stub.get_topic/3

      error = %GRPC.RPCError{message: "Topic not found", status: 5}

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.GetTopicRequest{
                                    topic: "projects/test/topics/topic"
                                  },
                                  ^fun ->
        {:error, error}
      end)

      assert {:error, ^error} = Topic.get(project: "test", topic: "topic")
    end
  end

  describe "id/1" do
    test "returns a properly formatted id string" do
      assert Topic.id(project: "my-project", topic: "test-topic") ==
               "projects/my-project/topics/test-topic"
    end

    test "raises an error if topic missing" do
      catch_error(Topic.id(project: "my-project"))
    end

    test "raises an error if project missing" do
      catch_error(Topic.id(topic: "my-topic"))
    end
  end

  describe "publish/2" do
    test "publishes single message" do
      fun = &Google.Pubsub.V1.Publisher.Stub.publish/3

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.PublishRequest{
                                    topic: "projects/test/topics/topic",
                                    messages: [
                                      %Google.Pubsub.V1.PubsubMessage{
                                        data: "Hello world"
                                      }
                                    ]
                                  },
                                  ^fun ->
        {:ok, %Google.Pubsub.V1.PublishResponse{}}
      end)

      assert Topic.publish(%Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"}, %Message{
               data: "Hello world"
             }) == :ok
    end

    test "publishes multiple messages" do
      fun = &Google.Pubsub.V1.Publisher.Stub.publish/3

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.PublishRequest{
                                    topic: "projects/test/topics/topic",
                                    messages: [
                                      %Google.Pubsub.V1.PubsubMessage{
                                        data: "Hello world"
                                      },
                                      %Google.Pubsub.V1.PubsubMessage{
                                        data: "Hello world 2"
                                      },
                                      %Google.Pubsub.V1.PubsubMessage{
                                        data: "Hello world 3"
                                      }
                                    ]
                                  },
                                  ^fun ->
        {:ok, %Google.Pubsub.V1.PublishResponse{}}
      end)

      assert Topic.publish(%Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"}, [
               %Message{data: "Hello world"},
               %Message{data: "Hello world 2"},
               %Message{data: "Hello world 3"}
             ]) == :ok
    end

    test "returns an error if the publish fails" do
      fun = &Google.Pubsub.V1.Publisher.Stub.publish/3

      error = %GRPC.RPCError{status: 5, message: "Topic not found"}

      Google.Pubsub.ClientMock
      |> expect(:send_request, fn %Google.Pubsub.V1.PublishRequest{
                                    topic: "projects/test/topics/topic",
                                    messages: [
                                      %Google.Pubsub.V1.PubsubMessage{
                                        data: "Hello world"
                                      },
                                      %Google.Pubsub.V1.PubsubMessage{
                                        data: "Hello world 2"
                                      },
                                      %Google.Pubsub.V1.PubsubMessage{
                                        data: "Hello world 3"
                                      }
                                    ]
                                  },
                                  ^fun ->
        {:error, error}
      end)

      assert {:error, ^error} =
               Topic.publish(%Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"}, [
                 %Message{data: "Hello world"},
                 %Message{data: "Hello world 2"},
                 %Message{data: "Hello world 3"}
               ])
    end
  end
end
