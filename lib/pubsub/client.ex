defmodule Google.Pubsub.Client do
  alias Google.Pubsub.Connection

  alias Google.Pubsub.V1.{
    Topic,
    Publisher,
    GetTopicRequest,
    PublishRequest,
    PubsubMessage,
    PublishResponse,
    Subscription,
    Subscriber,
    GetSubscriptionRequest,
    DeleteSubscriptionRequest,
    PullRequest,
    AcknowledgeRequest
  }

  @spec create_topic(String.t()) :: {:ok, Topic.t()} | {:error, any()}
  def create_topic(id) do
    Topic.new(name: id)
    |> send_request(&Publisher.Stub.create_topic/3)
  end

  @spec get_topic(String.t()) :: {:ok, Topic.t()} | {:error, any()}
  def get_topic(id) do
    GetTopicRequest.new(topic: id)
    |> send_request(&Publisher.Stub.get_topic/3)
  end

  @spec publish(String.t(), [PubsubMessage.t()]) :: {:ok, PublishResponse.t()} | {:error, any()}
  def publish(topic_id, messages) do
    PublishRequest.new(topic: topic_id, messages: messages)
    |> send_request(&Publisher.Stub.publish/3)
  end

  @spec create_subscription(String.t(), String.t()) :: {:ok, Subscription.t()} | {:error, any()}
  def create_subscription(topic_id, subscription_id) do
    Subscription.new(
      topic: topic_id,
      name: subscription_id
    )
    |> send_request(&Subscriber.Stub.create_subscription/3)
  end

  @spec get_subscription(String.t()) :: {:ok, Subscription.t()} | {:error, any()}
  def get_subscription(subscription_id) do
    GetSubscriptionRequest.new(subscription: subscription_id)
    |> send_request(&Subscriber.Stub.get_subscription/3)
  end

  def delete_subscription(subscription_id) do
    DeleteSubscriptionRequest.new(subscription: subscription_id)
    |> send_request(&Subscriber.Stub.delete_subscription/3)
  end

  def pull(subscription_id, max_messages \\ 10) do
    PullRequest.new(
      subscription: subscription_id,
      max_messages: max_messages
    )
    |> send_request(&Subscriber.Stub.pull/3)
  end

  def streaming_pull() do
    stub(&Subscriber.Stub.streaming_pull/2, timeout: :infinity)
  end

  def acknowledge(subscription_id, ack_ids) do
    AcknowledgeRequest.new(ack_ids: ack_ids, subscription: subscription_id)
    |> send_request(&Subscriber.Stub.acknowledge/3)
  end

  @spec stub(function(), Keyword.t()) :: {:ok, any()} | {:error, any()}
  defp stub(fun, opts) do
    {timeout, opts} = Keyword.pop(opts, :conn_timeout, 10_000)

    :poolboy.transaction(
      :grpc_connection_pool,
      fn pid ->
        Connection.get(pid)
        |> fun.(request_opts(opts))
      end,
      timeout
    )
  end

  @spec send_request(any(), function(), Keyword.t()) :: {:ok, any()} | {:error, any()}
  defp send_request(req, fun, opts \\ []) do
    stub(
      fn conn, req_opts ->
        fun.(conn, req, req_opts)
      end,
      opts
    )
  end

  @spec request_opts(Keyword.t()) :: Keyword.t()
  defp request_opts(opts) do
    opts = Keyword.put(opts, :content_type, "application/grpc")

    case auth_token() do
      {:ok, %{token: token, type: token_type}} ->
        Keyword.put(opts, :metadata, %{
          "authorization" => "#{token_type} #{token}"
        })

      _ ->
        opts
    end
  end

  defp auth_token() do
    case Application.get_env(:google_grpc_pubsub, :goth) do
      nil -> {:ok, nil}
      mod -> Goth.fetch(mod)
    end
  end
end
