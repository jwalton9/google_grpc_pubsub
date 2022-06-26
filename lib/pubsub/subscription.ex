defmodule Pubsub.Subscription do
  alias Pubsub.{Client, Topic}
  alias Google.Pubsub.V1.Subscriber.Stub

  @type t :: %__MODULE__{
          id: String.t()
        }

  defstruct [:id]

  @spec new(project: String.t(), name: String.t()) :: t()
  def new(opts) do
    %__MODULE__{id: id(opts[:project], opts[:name])}
  end

  @spec create(t(), Topic.t()) ::
          {:ok, Google.Pubsub.V1.Subscription.t()} | {:error, any()}
  def create(struct, topic) do
    subscription =
      Google.Pubsub.V1.Subscription.new(
        name: struct.id,
        topic: topic.id
      )

    Client.call(Stub, :create_subscription, subscription)
  end

  @spec delete(t()) :: Google.Protobuf.Empty.t()
  def delete(struct) do
    request = Google.Pubsub.V1.DeleteSubscriptionRequest.new(subscription: struct.id)

    Client.call(Stub, :delete_subscription, request)
  end

  @spec id(String.t(), String.t()) :: String.t()
  defp id(project, name) do
    Path.join(["projects", project, "subscriptions", name])
  end
end
