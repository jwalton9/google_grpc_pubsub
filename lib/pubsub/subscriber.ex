defmodule Google.Pubsub.Subscriber do
  alias Google.Pubsub.{Message, Subscription, Client}

  alias Google.Pubsub.V1.{
    StreamingPullRequest,
    StreamingPullResponse
  }

  @type t :: %__MODULE__{
          subscription: Subscription.t(),
          request_opts: Keyword.t()
        }

  defstruct subscription: nil, request_opts: []

  @callback handle_messages([Message.t()], Subscription.t()) :: [Message.t()]

  @unavailable GRPC.Status.unavailable()
  @unknown GRPC.Status.unknown()

  defmacro __using__(_opts) do
    quote do
      use GenServer

      require Logger

      alias Google.Pubsub.{Subscriber, Subscription}

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
        |> Subscriber.create_stream(request_opts)
        |> Subscriber.receive_messages(subscription, &handle_messages/2)
        |> Subscriber.close_stream(subscription)

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
    end
  end

  @spec create_stream(Subscription.t(), Keyword.t()) :: GRPC.Client.Stream.t()
  def create_stream(subscription, request_opts) do
    request =
      StreamingPullRequest.new(
        subscription: subscription.name,
        stream_ack_deadline_seconds: Keyword.get(request_opts, :stream_ack_deadline_seconds, 10)
      )

    client().streaming_pull()
    |> GRPC.Stub.send_request(request)
  end

  @spec close_stream(GRPC.Client.Stream.t(), Subscription.t()) :: GRPC.Client.Stream.t()
  def close_stream(stream, subscription) do
    request = StreamingPullRequest.new(subscription: subscription.name)

    GRPC.Stub.send_request(stream, request, end_stream: true)
  end

  @spec receive_messages(GRPC.Client.Stream.t(), Subscription.t(), function()) ::
          GRPC.Client.Stream.t()
  def receive_messages(stream, subscription, handle_messages) do
    case GRPC.Stub.recv(stream, timeout: :infinity) do
      {:ok, recv} ->
        process_recv(recv, stream, subscription, handle_messages)

      {:error, %GRPC.RPCError{status: @unknown, message: message} = e} ->
        if expected_error?(message), do: [], else: raise(e)

      {:error, error} ->
        raise error
    end
  end

  @spec process_recv(Enumerable.t(), GRPC.Client.Stream.t(), Subscription.t(), function()) ::
          GRPC.Client.Stream.t()
  defp process_recv(recv, stream, subscription, handle_messages) do
    Enum.reduce_while(recv, stream, fn
      {:ok, %StreamingPullResponse{received_messages: received_messages}}, stream ->
        ack_ids =
          received_messages
          |> Enum.map(&Message.new!/1)
          |> handle_messages.(subscription)
          |> Enum.map(fn %Message{ack_id: ack_id} -> ack_id end)

        {:cont, ack(stream, ack_ids)}

      {:error, %GRPC.RPCError{status: @unavailable}}, stream ->
        {:cont, stream}

      {:error, %GRPC.RPCError{status: @unknown, message: message}}, stream ->
        if expected_error?(message), do: {:cont, stream}, else: {:halt, stream}

      {:error, _error}, _stream ->
        {:halt, stream}
    end)
  end

  @spec ack(GRPC.Client.Stream.t(), [String.t()]) :: GRPC.Client.Stream.t()
  defp ack(stream, ack_ids) do
    request = StreamingPullRequest.new(ack_ids: ack_ids)

    GRPC.Stub.send_request(stream, request)
  end

  defp client(), do: Application.get_env(:google_grpc_pubsub, :client, Client)

  defp expected_error?(message) do
    Regex.match?(~r/goaway.*max_age/, message) or
      Regex.match?(~r/stream_error.*Stream reset by server/, message)
  end
end
