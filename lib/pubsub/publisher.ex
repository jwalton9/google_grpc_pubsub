defmodule Pubsub.Publisher do
  alias Pubsub.Client
  alias Google.Pubsub.V1.{Publisher, Topic, PublishRequest, PublishResponse, PubsubMessage}

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

    case Client.send_request(Publisher.Stub, :publish, request) do
      {:ok, %PublishResponse{}} -> :ok
      {:error, error} -> {:error, error}
    end
  end
end
