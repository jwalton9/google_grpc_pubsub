defmodule Google.Pubsub.Client do
  alias Google.Pubsub.Connection

  @callback send_request(function()) :: {:ok, any()} | {:error, any()}

  @callback send_request(function(), Keyword.t()) :: {:ok, any()} | {:error, any()}

  @callback send_request(any(), function()) :: {:ok, any()} | {:error, any()}

  @callback send_request(any(), function(), Keyword.t()) :: {:ok, any()} | {:error, any()}

  @spec send_request(function()) :: {:ok, any()} | {:error, any()}
  def send_request(fun), do: send_request(fun, [])

  @spec send_request(function(), Keyword.t()) :: {:ok, any()} | {:error, any()}
  def send_request(fun, opts) when is_function(fun, 2) do
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

  @spec send_request(any(), function()) :: {:ok, any()} | {:error, any()}
  def send_request(req, fun) when is_function(fun, 3), do: send_request(req, fun, [])

  @spec send_request(any(), function(), Keyword.t()) :: {:ok, any()} | {:error, any()}
  def send_request(req, fun, opts) when is_function(fun, 3) do
    send_request(fn channel, opts -> fun.(channel, req, opts) end, opts)
  end

  @spec request_opts(Keyword.t()) :: Keyword.t()
  defp request_opts(opts) do
    case auth_token() do
      {:ok, %{token: token, type: token_type}} ->
        Keyword.put(opts, :metadata, %{"authorization" => "#{token_type} #{token}"})

      _ ->
        opts
    end
  end

  defp auth_token() do
    if Application.get_env(:goth, :disabled, false) do
      {:ok, nil}
    else
      Goth.Token.for_scope("https://www.googleapis.com/auth/pubsub")
    end
  end
end
