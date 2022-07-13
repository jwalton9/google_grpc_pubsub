defmodule Google.Pubsub.Subscriber do
  alias Google.Pubsub.{Message, Subscription}

  @type t :: %__MODULE__{
          subscription: Subscription.t(),
          request_opts: Keyword.t()
        }

  defstruct subscription: nil, request_opts: []

  @callback handle_messages([Message.t()]) :: [Message.t()]

  defmacro __using__(_opts) do
    quote do
      use GenServer

      require Logger

      alias Google.Pubsub.{Message, Subscriber, Subscription, Client}

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

        {subscription_opts, request_opts} = Keyword.split(init_arg, [:subscription, :project])

        case Subscription.get(
               project: subscription_opts[:project],
               subscription: subscription_opts[:subscription]
             ) do
          {:ok, subscription} ->
            {:ok, %Subscriber{subscription: subscription, request_opts: request_opts}}

          {:error, error} ->
            {:error, error}
        end
      end

      @impl true
      def handle_info(
            :listen,
            %Subscriber{subscription: subscription, request_opts: request_opts} = state
          ) do
        subscription
        |> create_stream(request_opts)
        |> receive_messages()
        |> close_stream(subscription)

        schedule_listen()

        {:noreply, state}
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
        Logger.debug("Google.Pubsub.Subscriber: handle_info: #{inspect(type)}")
        {:stop, :unknown, struct}
      end

      defp schedule_listen() do
        Process.send_after(self(), :listen, 100)
      end

      @spec create_stream(Subscription.t(), Keyword.t()) :: GRPC.Client.Stream.t()
      defp create_stream(subscription, request_opts) do
        request =
          StreamingPullRequest.new(
            subscription: subscription.name,
            stream_ack_deadline_seconds:
              Keyword.get(request_opts, :stream_ack_deadline_seconds, 10)
          )

        Client.send_request(&Stub.streaming_pull/2, timeout: :infinity)
        |> GRPC.Stub.send_request(request)
      end

      @spec close_stream(GRPC.Client.Stream.t(), Subscription.t()) :: GRPC.Client.Stream.t()
      defp close_stream(stream, subscription) do
        request = StreamingPullRequest.new(subscription: subscription.name)

        GRPC.Stub.send_request(stream, request, end_stream: true)
      end

      @spec receive_messages(GRPC.Client.Stream.t()) :: GRPC.Client.Stream.t()
      defp receive_messages(stream) do
        case GRPC.Stub.recv(stream, timeout: :infinity) do
          {:ok, recv} ->
            process_recv(recv, stream)

          {:error, error} ->
            raise error
        end
      end

      @spec process_recv(Enumerable.t(), GRPC.Client.Stream.t()) :: GRPC.Client.Stream.t()
      defp process_recv(recv, stream) do
        Enum.reduce(recv, stream, fn
          {:ok, %StreamingPullResponse{received_messages: received_messages}}, stream ->
            ack_ids =
              received_messages
              |> Enum.map(&Message.new!/1)
              |> handle_messages()
              |> Enum.map(fn %Message{ack_id: ack_id} -> ack_id end)

            ack(stream, ack_ids)

          {:error, error}, _stream ->
            raise error
        end)
      end

      @spec ack(GRPC.Client.Stream.t(), [String.t()]) :: GRPC.Client.Stream.t()
      defp ack(stream, ack_ids) do
        request = StreamingPullRequest.new(ack_ids: ack_ids)

        GRPC.Stub.send_request(stream, request)
      end
    end
  end
end
