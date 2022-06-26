defmodule Pubsub.Subscription do
  alias Pubsub.{Client, Topic}
  alias Google.Pubsub.V1.{Subscriber.Stub, Subscription, GetSubscriptionRequest}

  @type opts :: [project: String.t(), subscription: String.t()]

  @spec create(project: String.t(), subscription: String.t(), topic: String.t()) ::
          {:ok, Subscription.t()} | {:error, any()}
  def create(opts) do
    subscription =
      Subscription.new(
        name: id(opts),
        topic: Topic.id(opts)
      )

    Client.send_request(Stub, :create_subscription, subscription)
  end

  @spec get(opts()) ::
          {:ok, Subscription.t()} | {:error, any()}
  def get(opts) do
    request = GetSubscriptionRequest.new(subscription: id(opts))

    Client.send_request(Stub, :get_subscription, request)
  end

  @spec delete(Subscription.t()) :: Google.Protobuf.Empty.t()
  def delete(%Subscription{name: name}) do
    request = Google.Pubsub.V1.DeleteSubscriptionRequest.new(subscription: name)

    Client.send_request(Stub, :delete_subscription, request)
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
end
