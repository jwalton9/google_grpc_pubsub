defmodule Google.Pubsub.Topic do
  alias Google.Pubsub.{Client, Message}

  alias Google.Pubsub.V1.{
    Publisher.Stub,
    Topic,
    PubsubMessage,
    GetTopicRequest,
    PublishRequest,
    PublishResponse
  }

  @type t :: Topic.t()

  @type opts :: [project: String.t(), topic: String.t()]

  @spec create(opts()) :: {:ok, t()} | {:error, any()}
  def create(opts) do
    request = Topic.new(name: id(opts))

    client().send_request(request, &Stub.create_topic/3)
  end

  @spec get(opts()) :: {:ok, t()} | {:error, any()}
  def get(opts) do
    request = GetTopicRequest.new(topic: id(opts))

    client().send_request(request, &Stub.get_topic/3)
  end

  @spec id(opts()) :: String.t()
  def id(opts) do
    Path.join(["projects", Keyword.fetch!(opts, :project), "topics", Keyword.fetch!(opts, :topic)])
  end

  @spec publish(t(), [Message.t()]) :: :ok | {:error, any()}
  def publish(topic, message) when not is_list(message),
    do: publish(topic, [message])

  def publish(topic, messages) when is_list(messages) do
    messages =
      messages
      |> Enum.map(fn %Message{data: data} when is_binary(data) ->
        PubsubMessage.new(data: data)
      end)

    request = PublishRequest.new(topic: topic.name, messages: messages)

    case client().send_request(request, &Stub.publish/3) do
      {:ok, %PublishResponse{}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp client(), do: Application.get_env(:google_grpc_pubsub, :client, Client)
end
