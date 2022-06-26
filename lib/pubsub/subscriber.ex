defmodule Pubsub.Subscriber do
  alias Pubsub.{Client, Subscription, Message}

  alias Google.Pubsub.V1.{
    Subscriber.Stub,
    PullRequest,
    PullResponse,
    AcknowledgeRequest
  }

  @type t :: %__MODULE__{
          project: String.t(),
          subscription: String.t(),
          request_opts: Keyword.t()
        }

  defstruct [:project, :subscription, :request_opts]

  @callback handle_messages([Message.t()]) :: [Message.t()]

  defmacro __using__(_opts) do
    quote do
      use GenServer

      require Logger

      import Pubsub.Message, only: [ack: 1]

      alias Pubsub.Subscriber

      alias Google.Pubsub.V1.{
        Subscriber.Stub,
        StreamingPullRequest,
        StreamingPullResponse
      }

      @behaviour Subscriber

      def child_spec(init_arg) do
        %{
          id: {Subscriber, init_arg[:project], init_arg[:subscription]},
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
         %Subscriber{
           project: project,
           subscription: subscription,
           request_opts: request_opts
         }}
      end

      @impl true
      def handle_info(:listen, struct) do
        create_stream(struct)
        |> receive_messages(&handle_messages/1)
        |> close_stream(struct)

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

      @spec create_stream(t()) :: GRPC.Client.Stream.t()
      defp create_stream(
             %Subscriber{
               request_opts: request_opts
             } = struct
           ) do
        request =
          request_opts
          |> Keyword.merge(
            subscription: subscription_path(struct),
            stream_ack_deadline_seconds: 10
          )
          |> StreamingPullRequest.new()

        Client.channel()
        |> Stub.streaming_pull(timeout: :infinity)
        |> GRPC.Stub.send_request(request)
      end

      @spec receive_messages(GRPC.Client.Stream.t(), t()) :: GRPC.Client.Stream.t()
      defp close_stream(stream, struct) do
        request = StreamingPullRequest.new(subscription: subscription_path(struct))

        GRPC.Stub.send_request(stream, request, end_stream: true)
      end

      @spec receive_messages(GRPC.Client.Stream.t(), function()) :: GRPC.Client.Stream.t()
      defp receive_messages(stream, fun) do
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
      defp ack(stream, ack_ids) do
        request = StreamingPullRequest.new(ack_ids: ack_ids)

        GRPC.Stub.send_request(stream, request)
      end
    end
  end

  @spec pull(Subscription.t(), max_messages: number()) :: {:ok, [Message.t()]} | {:error, any()}
  def pull(subscription, opts \\ []) do
    request =
      PullRequest.new(
        subscription: subscription.id,
        max_messages: Keyword.get(opts, :max_messages, 10)
      )

    Client.call(Stub, :pull, request)
    |> case do
      {:ok, %PullResponse{received_messages: received_messages}} ->
        {:ok, Enum.map(received_messages, &Message.new/1)}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec acknowledge(Subscription.t(), [Message.t()]) ::
          {:ok, Google.Protobuf.Empty.t()} | {:error, any()}
  def acknowledge(subscription, messages) do
    ack_ids =
      messages
      |> Enum.filter(fn %Message{acked?: acked} -> acked end)
      |> Enum.map(fn message -> message.ack_id end)

    request = AcknowledgeRequest.new(subscription: subscription.id, ack_ids: ack_ids)

    Client.call(Stub, :acknowledge, request)
  end
end
