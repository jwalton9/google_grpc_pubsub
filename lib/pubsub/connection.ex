defmodule Pubsub.Connection do
  use GenServer

  def start_link(init) do
    GenServer.start_link(__MODULE__, init)
  end

  @spec get(pid()) :: {GRPC.Channel.t(), Keyword.t()}

  def get(pid) do
    GenServer.call(pid, :get_connection)
  end

  @impl true
  def init(_) do
    Process.flag(:trap_exit, true)

    connect()
  end

  @impl true
  def handle_call(:get_connection, _from, channel) do
    {:reply, channel, channel}
  end

  @impl true
  def handle_info({:EXIT, _from, reason}, channel) do
    disconnect(channel)
    {:stop, reason, channel}
  end

  def handle_info(_info, channel) do
    {:noreply, channel}
  end

  @impl true
  def terminate(_reason, channel) do
    disconnect(channel)
    channel
  end

  defp connect() do
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
  end

  defp disconnect(channel) do
    GRPC.Stub.disconnect(channel)
  end
end
