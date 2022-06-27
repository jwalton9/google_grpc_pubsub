defmodule Pubsub.Subscription do
  alias Pubsub.{Client, Topic, Message}

  alias Google.Pubsub.V1.{
    Subscriber.Stub,
    Subscription,
    GetSubscriptionRequest,
    PullRequest,
    PullResponse,
    AcknowledgeRequest
  }

  @type opts :: [project: String.t(), subscription: String.t()]

  @spec create(project: String.t(), subscription: String.t(), topic: String.t()) ::
          {:ok, Subscription.t()} | {:error, any()}
  def create(opts) do
    subscription =
      Subscription.new(
        name: id(opts),
        topic: Topic.id(opts)
      )

    Client.send_request(subscription, &Stub.create_subscription/3)
  end

  @spec get(opts()) ::
          {:ok, Subscription.t()} | {:error, any()}
  def get(opts) do
    request = GetSubscriptionRequest.new(subscription: id(opts))

    Client.send_request(request, &Stub.get_subscription/3)
  end

  @spec delete(Subscription.t()) :: Google.Protobuf.Empty.t()
  def delete(%Subscription{name: name}) do
    request = Google.Pubsub.V1.DeleteSubscriptionRequest.new(subscription: name)

    Client.send_request(request, &Stub.delete_subscription/3)
  end

  @spec id(opts()) :: String.t()
  def id(opts) do
    Path.join([
      "projects",
      Keyword.fetch!(opts, :project),
      "subscriptions",
      Keyword.fetch!(opts, :subscription)
    ])
  end

  @spec pull(Subscription.t(), max_messages: number()) :: {:ok, [Message.t()]} | {:error, any()}
  def pull(%Subscription{name: name}, opts \\ []) do
    request =
      PullRequest.new(
        subscription: name,
        max_messages: Keyword.get(opts, :max_messages, 10)
      )

    Client.send_request(request, &Stub.pull/3)
    |> case do
      {:ok, %PullResponse{received_messages: received_messages}} ->
        {:ok, Enum.map(received_messages, &Message.new/1)}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec acknowledge(Subscription.t(), [Message.t()]) ::
          :ok | {:error, any()}
  def acknowledge(subscription, messages) do
    ack_ids = Enum.map(messages, fn message -> message.ack_id end)

    request = AcknowledgeRequest.new(ack_ids: ack_ids, subscription: subscription.name)

    case Client.send_request(request, &Stub.acknowledge/3) do
      {:ok, %Google.Protobuf.Empty{}} -> :ok
      {:error, error} -> {:error, error}
    end
  end
end
