defmodule Pubsub.Client do
  @moduledoc false
  use GenServer

  def start_link(init_opts) do
    GenServer.start_link(__MODULE__, init_opts, name: __MODULE__)
  end

  @spec get() :: GRPC.Channel.t()
  def get() do
    GenServer.call(__MODULE__, :channel)
  end

  @spec send_request(module(), function(), any(), Keyword.t()) :: {:ok, any()} | {:error, any()}
  def send_request(mod, fun, req, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, 5000)

    GenServer.call(__MODULE__, {:send_request, mod, fun, req, opts}, timeout)
  end

  @spec request_opts(Keyword.t(), boolean()) :: Keyword.t()
  defp request_opts(opts, false), do: opts

  defp request_opts(opts, _) do
    case Goth.Token.for_scope("https://www.googleapis.com/auth/pubsub") do
      {:ok, %{token: token, type: token_type}} ->
        Keyword.put(opts, :metadata, %{"authorization" => "#{token_type} #{token}"})

      _ ->
        opts
    end
  end

  @impl true
  def init(_) do
    emulator = Application.get_env(:google_grpc_pubsub, :emulator)

    case emulator do
      nil ->
        ssl_opts =
          Application.get_env(:google_grpc_pubsub, :ssl_opts)
          |> Keyword.put(:cacerts, :certifi.cacerts())

        credentials = GRPC.Credential.new(ssl: ssl_opts)

        GRPC.Stub.connect("pubsub.googleapis.com", 443,
          cred: credentials,
          adapter_opts: %{
            http2_opts: %{keepalive: :infinity}
          }
        )

      {host, port} when is_binary(host) and is_number(port) ->
        GRPC.Stub.connect(host, port,
          adapter_opts: %{
            http2_opts: %{keepalive: :infinity}
          }
        )
    end
    |> case do
      {:ok, channel} ->
        {:ok, %{channel: channel, use_goth: is_nil(emulator), tasks: %{}}}

      {:error, error} ->
        {:error, error}
    end
  end

  @impl true
  def handle_call(:channel, _info, %{channel: channel} = state) do
    {:reply, channel, state}
  end

  def handle_call(
        {:send_request, mod, fun, req, opts},
        from,
        %{channel: channel, use_goth: use_goth} = state
      ) do
    opts = request_opts(opts, use_goth)

    task =
      Task.Supervisor.async_nolink(Pubsub.TaskSupervisor, fn ->
        apply(mod, fun, [channel, req, opts])
      end)

    state = put_in(state.tasks[task.ref], from)

    {:noreply, state}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) and is_map_key(state.tasks, ref) do
    Process.demonitor(ref, [:flush])

    {from, state} = pop_in(state.tasks[ref])

    GenServer.reply(from, result)

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, _, _, reason}, state)
      when is_reference(ref) and is_map_key(state.tasks, ref) do
    {from, state} = pop_in(state.tasks[ref])

    GenServer.reply(from, {:error, reason})

    {:noreply, state}
  end

  def handle_info(_info, state) do
    {:noreply, state}
  end
end
