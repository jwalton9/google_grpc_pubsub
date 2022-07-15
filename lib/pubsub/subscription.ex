defmodule Google.Pubsub.Subscription do
  alias Google.Pubsub.{Client, Topic, Message}

  alias Google.Pubsub.V1.{Subscription, PullResponse}

  @type t :: Subscription.t()

  @type opts :: [project: String.t(), subscription: String.t()]

  @spec create(project: String.t(), subscription: String.t(), topic: String.t()) ::
          {:ok, t()} | {:error, any()}
  def create(opts) do
    topic_id = Topic.id(opts)
    subscription_id = id(opts)

    client().create_subscription(topic_id, subscription_id)
  end

  @spec get(opts()) ::
          {:ok, t()} | {:error, any()}
  def get(opts) do
    opts |> id() |> client().get_subscription()
  end

  @spec delete(t()) :: :ok | {:error, any()}
  def delete(%Subscription{name: name}) do
    name
    |> client().delete_subscription()
    |> case do
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
    max_messages = Keyword.get(opts, :max_messages, 10)

    case client().pull(name, max_messages) do
      {:ok, %PullResponse{received_messages: received_messages}} ->
        {:ok, Enum.map(received_messages, &Message.new!/1)}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec acknowledge(t(), Message.t() | [Message.t()]) ::
          :ok | {:error, any()}
  def acknowledge(%Subscription{name: name}, messages) when is_list(messages) do
    ack_ids = Enum.map(messages, fn message -> message.ack_id end)

    case client().acknowledge(name, ack_ids) do
      {:ok, %Google.Protobuf.Empty{}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def acknowledge(subscription, message), do: acknowledge(subscription, [message])

  defp client(), do: Application.get_env(:google_grpc_pubsub, :client, Client)
end
