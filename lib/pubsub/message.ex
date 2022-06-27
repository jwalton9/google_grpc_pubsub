defmodule Google.Pubsub.Message do
  alias Google.Pubsub.V1.{ReceivedMessage, PubsubMessage}

  @type t :: %__MODULE__{
          ack_id: String.t(),
          data: String.t()
        }

  defstruct ack_id: nil, data: nil

  @spec new!(ReceivedMessage.t() | String.t() | map()) :: t()
  def new!(%ReceivedMessage{ack_id: ack_id, message: %PubsubMessage{data: data}}) do
    %__MODULE__{
      ack_id: ack_id,
      data: data
    }
  end

  def new!(data) when is_binary(data) do
    %__MODULE__{
      data: data
    }
  end

  def new!(data) when is_map(data) do
    data |> Poison.encode!() |> Base.encode64() |> new!()
  end

  @spec decode(t()) :: {:ok, map()} | {:error, any()}
  def decode(message) do
    message.data
    |> Base.decode64()
    |> case do
      {:ok, data} -> Poison.decode(data)
      :error -> {:error, "Invalid base64 encoded data: #{inspect(message.data)}"}
    end
  end

  @spec decode!(t()) :: map()
  def decode!(message) do
    case decode(message) do
      {:ok, decoded_message} ->
        decoded_message

      {:error, error} ->
        raise error
    end
  end
end
