defmodule Pubsub.Topic do
  alias Pubsub.Client
  alias Google.Pubsub.V1.{Publisher.Stub, Topic, GetTopicRequest}

  @type opts :: [project: String.t(), topic: String.t()]

  @spec create(opts()) :: {:ok, Topic.t()} | {:error, any()}
  def create(opts) do
    request = Topic.new(name: id(opts))

    Client.send_request(Stub, :create_topic, request)
  end

  @spec get(opts()) :: {:ok, Topic.t()} | {:error, any()}
  def get(opts) do
    request = GetTopicRequest.new(topic: id(opts))

    Client.send_request(Stub, :get_topic, request)
  end

  @spec id(opts()) :: String.t()
  def id(opts) do
    Path.join(["projects", Keyword.fetch!(opts, :project), "topics", Keyword.fetch!(opts, :topic)])
  end
end
