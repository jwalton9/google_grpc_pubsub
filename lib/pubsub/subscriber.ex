defmodule Pubsub.Subscriber do
  alias Pubsub.Client

  alias Google.Pubsub.V1.{
    StreamingPullRequest,
    Subscriber,
    StreamingPullResponse,
    Subscription,
    DeleteSubscriptionRequest
  }

  @type t :: %__MODULE__{
          project: String.t(),
          subscription: String.t(),
          request_opts: Keyword.t()
        }

  defstruct [:project, :subscription, :request_opts]

  @callback handle_messages([Pubsub.Message.t()]) :: [Pubsub.Message.t()]

  defmacro __using__(_opts) do
    quote do
      use GenServer

      require Logger

      import Pubsub.Message

      alias Pubsub.Message

      @behaviour Pubsub.Subscriber

      def child_spec(init_arg) do
        %{
          id: {Pubsub.Subscriber, init_arg[:project], init_arg[:subscription]},
          start: {__MODULE__, :start_link, [init_arg]}
        }
      end

      def start_link(init_arg) do
        GenServer.start_link(__MODULE__, init_arg)
      end

      @impl true
      def init(init_arg) do
        schedule_listen()

        {subscription, request_opts} = Keyword.pop!(init_arg, :subscription)

        {project, request_opts} = Keyword.pop!(request_opts, :project)

        {:ok,
         %Pubsub.Subscriber{
           project: project,
           subscription: subscription,
           request_opts: request_opts
         }}
      end

      @impl true
      def handle_info(:listen, struct) do
        Pubsub.Subscriber.create_stream(struct)
        |> Pubsub.Subscriber.receive_messages(&handle_messages/1)
        |> Pubsub.Subscriber.close_stream(struct)

        schedule_listen()

        {:noreply, struct}
      end

      @impl true
      def handle_info({:gun_error, _, _, {:stream_error, :no_error, _}}, struct) do
        {:stop, :shutdown, struct}
      end

      @impl true
      def handle_info({:gun_error, _, _, {:badstate, _}}, struct) do
        {:stop, :shutdown, struct}
      end

      @impl true
      def handle_info(type, struct) do
        Logger.debug("Pubsub.Subscriber: handle_info: #{inspect(type)}")
        {:stop, :unknown, struct}
      end

      defp schedule_listen() do
        Process.send_after(self(), :listen, 100)
      end
    end
  end

  @spec create_subscription(t(), String.t()) :: Subscription.t()
  def create_subscription(%__MODULE__{project: project} = struct, topic) do
    subscription =
      Subscription.new(
        name: subscription_path(struct),
        topic: Pubsub.Publisher.topic_path(%Pubsub.Publisher{project: project, topic: topic})
      )

    Client.call(Subscriber.Stub, :create_subscription, subscription)
  end

  @spec delete_subscription(t()) :: Google.Protobuf.Empty.t()
  def delete_subscription(struct) do
    request = DeleteSubscriptionRequest.new(subscription: subscription_path(struct))

    Client.call(Subscriber.Stub, :delete_subscription, request)
  end

  @spec create_stream(t()) :: GRPC.Client.Stream.t()
  def create_stream(
        %__MODULE__{
          request_opts: request_opts
        } = struct
      ) do
    request =
      request_opts
      |> Keyword.merge(subscription: subscription_path(struct), stream_ack_deadline_seconds: 10)
      |> StreamingPullRequest.new()

    Client.channel()
    |> Subscriber.Stub.streaming_pull(timeout: :infinity)
    |> GRPC.Stub.send_request(request)
  end

  @spec receive_messages(GRPC.Client.Stream.t(), t()) :: GRPC.Client.Stream.t()
  def close_stream(stream, struct) do
    request = StreamingPullRequest.new(subscription: subscription_path(struct))

    GRPC.Stub.send_request(stream, request, end_stream: true)
  end

  @spec receive_messages(GRPC.Client.Stream.t(), function()) :: GRPC.Client.Stream.t()
  def receive_messages(stream, fun) do
    case GRPC.Stub.recv(stream, timeout: :infinity) do
      {:ok, recv} ->
        recv
        |> Stream.map(fn
          {:ok, %StreamingPullResponse{received_messages: received_messages}} ->
            received_messages
            |> Enum.map(&Pubsub.Message.new/1)

          {:error, error} ->
            raise error
        end)
        |> Enum.reduce(stream, fn stream, messages ->
          ack_ids =
            messages
            |> fun.()
            |> Enum.filter(fn %Pubsub.Message{acked?: acked} -> acked end)
            |> Enum.map(fn %Pubsub.Message{ack_id: ack_id} -> ack_id end)

          ack(stream, ack_ids)
        end)

      {:error, error} ->
        raise error
    end
  end

  @spec ack(GRPC.Client.Stream.t(), [String.t()]) :: GRPC.Client.Stream.t()
  def ack(stream, ack_ids) do
    request = StreamingPullRequest.new(ack_ids: ack_ids)

    GRPC.Stub.send_request(stream, request)
  end

  @spec subscription_path(t()) :: String.t()
  def subscription_path(%__MODULE__{project: project, subscription: subscription}) do
    Path.join(["projects", project, "subscriptions", subscription])
  end
end
