defmodule Google.Pubsub.Subscription do
  alias Google.Pubsub.{Client, Topic, Message}

  alias Google.Pubsub.V1.{
    Subscriber.Stub,
    Subscription,
    GetSubscriptionRequest,
    PullRequest,
    PullResponse,
    AcknowledgeRequest
  }

  @type t :: Subscription.t()

  @type opts :: [project: String.t(), subscription: String.t()]

  @spec create(project: String.t(), subscription: String.t(), topic: String.t()) ::
          {:ok, t()} | {:error, any()}
  def create(opts) do
    subscription =
      Subscription.new(
        name: id(opts),
        topic: Topic.id(opts)
      )

    client().send_request(subscription, &Stub.create_subscription/3)
  end

  @spec get(opts()) ::
          {:ok, t()} | {:error, any()}
  def get(opts) do
    request = GetSubscriptionRequest.new(subscription: id(opts))

    client().send_request(request, &Stub.get_subscription/3)
  end

  @spec delete(t()) :: :ok | {:error, any()}
  def delete(%Subscription{name: name}) do
    request = Google.Pubsub.V1.DeleteSubscriptionRequest.new(subscription: name)

    case client().send_request(request, &Stub.delete_subscription/3) do
      {:ok, %Google.Protobuf.Empty{}} -> :ok
      {:error, error} -> {:error, error}
    end
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

  @spec pull(t(), max_messages: number()) :: {:ok, [Message.t()]} | {:error, any()}
  def pull(%Subscription{name: name}, opts \\ []) do
    request =
      PullRequest.new(
        subscription: name,
        max_messages: Keyword.get(opts, :max_messages, 10)
      )

    client().send_request(request, &Stub.pull/3)
    |> case do
      {:ok, %PullResponse{received_messages: received_messages}} ->
        {:ok, Enum.map(received_messages, &Message.new!/1)}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec acknowledge(t(), Message.t() | [Message.t()]) ::
          :ok | {:error, any()}
  def acknowledge(subscription, messages) when is_list(messages) do
    ack_ids = Enum.map(messages, fn message -> message.ack_id end)

    request = AcknowledgeRequest.new(ack_ids: ack_ids, subscription: subscription.name)

    case client().send_request(request, &Stub.acknowledge/3) do
      {:ok, %Google.Protobuf.Empty{}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def acknowledge(subscription, message), do: acknowledge(subscription, [message])

  defp client(), do: Application.get_env(:google_grpc_pubsub, :client, Client)
end
