defmodule Pubsub.Publisher do
  alias Pubsub.{Topic, Client}
  alias Google.Pubsub.V1.{Publisher, PublishRequest, PublishResponse, PubsubMessage}

  @spec publish(Topic.t(), String.t() | map()) :: {:ok, PublishResponse.t()} | {:error, any()}
  def publish(struct, message) when not is_list(message),
    do: publish(struct, [message])

  @spec publish(Topic.t(), [String.t() | map()]) :: {:ok, PublishResponse.t()} | {:error, any()}
  def publish(struct, messages) when is_list(messages) do
    messages =
      Enum.map(messages, fn
        message when is_binary(message) ->
          message

        message when is_map(message) ->
          Poison.encode!(message)
      end)
      |> Enum.map(fn data -> PubsubMessage.new(data: data) end)

    request = PublishRequest.new(topic: struct.id, messages: messages)

    Client.call(Publisher.Stub, :publish, request)
  end
end
