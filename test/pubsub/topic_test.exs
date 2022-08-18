defmodule Google.Pubsub.TopicTest do
  use ExUnit.Case
  import Google.Pubsub.Testing

  alias Google.Pubsub.{Topic, Message}

  describe "create/1" do
    test "should create a topic project and topic provided" do
      topic = %Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"}

      assert {:ok, ^topic} = Topic.create(project: "test", topic: "topic")

      assert_topic_created("projects/test/topics/topic")
    end
  end

  describe "get/1" do
    test "should get a topic when project and topic provided" do
      topic = %Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"}

      assert {:ok, ^topic} = Topic.get(project: "test", topic: "topic")
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
      message = Message.new!("Hello world")

      assert Topic.publish(
               %Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"},
               message
             ) == :ok

      assert_messages_published("projects/test/topics/topic", [
        ^message
      ])
    end

    test "publishes multiple messages" do
      messages = [
        Message.new!(%{hello: "world"}),
        Message.new!("Hello world 2"),
        Message.new!("Hello world 3")
      ]

      assert Topic.publish(%Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"}, messages) ==
               :ok

      assert_messages_published("projects/test/topics/topic", ^messages)
    end

    test "publishes messages with attributes" do
      assert Topic.publish(%Google.Pubsub.V1.Topic{name: "projects/test/topics/topic"}, [
               Message.new!(%{hello: "world"}, %{type: "foo"}),
               Message.new!(%{hello: "world"}, %{type: "bar"})
             ]) ==
               :ok

      messages = [
        Message.new!(%{hello: "world"}, %{type: "foo"}),
        Message.new!(%{hello: "world"}, %{type: "bar"})
      ]

      assert_messages_published(
        "projects/test/topics/topic",
        ^messages
      )
    end
  end
end
