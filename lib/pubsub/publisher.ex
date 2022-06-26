defmodule Pubsub.Publisher do
  alias Pubsub.Client
  alias Google.Pubsub.V1.{Publisher, PublishRequest, PublishResponse, PubsubMessage, Topic}

  @type t :: %__MODULE__{
          project: String.t(),
          topic: String.t()
        }

  defstruct [:project, :topic]

  @spec create_topic(t()) :: {:ok, Topic.t()} | {:error, any()}
  def create_topic(struct) do
    topic = Topic.new(name: topic_path(struct))

    Client.call(Publisher.Stub, :create_topic, topic)
  end

  @spec publish(t(), String.t() | map()) :: {:ok, PublishResponse.t()} | {:error, any()}
  def publish(struct, message) when not is_list(message),
    do: publish(struct, [message])

  @spec publish(t(), [String.t() | map()]) :: {:ok, PublishResponse.t()} | {:error, any()}
  def publish(struct, messages) when is_list(messages) do
    messages =
      Enum.map(messages, fn
        message when is_binary(message) ->
          message

        message when is_map(message) ->
          Poison.encode!(message)
      end)
      |> Enum.map(fn data -> PubsubMessage.new(data: data) end)

    request = PublishRequest.new(topic: topic_path(struct), messages: messages)

    Client.call(Publisher.Stub, :publish, request)
  end

  @spec topic_path(t()) :: String.t()
  def topic_path(%__MODULE__{project: project, topic: topic}) do
    Path.join(["projects", project, "topics", topic])
  end
end
