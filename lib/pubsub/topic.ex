defmodule Pubsub.Topic do
  alias Pubsub.Client

  alias Google.Pubsub.V1.{
    Publisher.Stub,
    Topic,
    PubsubMessage,
    GetTopicRequest,
    PublishRequest,
    PublishResponse
  }

  @type opts :: [project: String.t(), topic: String.t()]

  @spec create(opts()) :: {:ok, Topic.t()} | {:error, any()}
  def create(opts) do
    request = Topic.new(name: id(opts))

    Client.send_request(request, &Stub.create_topic/3)
  end

  @spec get(opts()) :: {:ok, Topic.t()} | {:error, any()}
  def get(opts) do
    request = GetTopicRequest.new(topic: id(opts))

    Client.send_request(request, &Stub.get_topic/3)
  end

  @spec id(opts()) :: String.t()
  def id(opts) do
    Path.join(["projects", Keyword.fetch!(opts, :project), "topics", Keyword.fetch!(opts, :topic)])
  end

  @spec publish(Topic.t(), String.t() | map() | [String.t() | map()]) :: :ok | {:error, any()}
  def publish(topic, message) when not is_list(message),
    do: publish(topic, [message])

  def publish(topic, messages) when is_list(messages) do
    messages =
      Enum.map(messages, fn
        message when is_binary(message) ->
          message

        message when is_map(message) ->
          Poison.encode!(message)
      end)
      |> Enum.map(fn data -> PubsubMessage.new(data: data) end)

    request = PublishRequest.new(topic: topic.name, messages: messages)

    case Client.send_request(request, &Stub.publish/3) do
      {:ok, %PublishResponse{}} -> :ok
      {:error, error} -> {:error, error}
    end
  end
end
