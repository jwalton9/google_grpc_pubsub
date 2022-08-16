defmodule Google.Pubsub.Message do
  alias Google.Pubsub.V1.{ReceivedMessage, PubsubMessage}

  @type t :: %__MODULE__{
          ack_id: String.t(),
          data: String.t(),
          attributes: map(),
          delivery_attempt: number()
        }

  defstruct ack_id: nil, data: nil, attributes: %{}, delivery_attempt: 0

  @spec new!(ReceivedMessage.t() | String.t() | map()) :: t()
  def new!(%ReceivedMessage{
        ack_id: ack_id,
        message: %PubsubMessage{data: data, attributes: attributes},
        delivery_attempt: delivery_attempt
      }) do
    %__MODULE__{
      ack_id: ack_id,
      data: data,
      delivery_attempt: delivery_attempt,
      attributes: attributes
    }
  end

  def new!(data) when is_binary(data) or is_map(data), do: new!(data, %{})

  @spec new!(binary() | map(), map()) :: t()
  def new!(data, attributes) when is_binary(data) do
    %__MODULE__{
      data: data,
      attributes: attributes
    }
  end

  def new!(data, attributes) when is_map(data) do
    data |> Poison.encode!() |> new!(attributes)
  end

  @spec decode(t()) :: {:ok, map()} | {:error, any()}
  def decode(message) do
    message.data
    |> Poison.decode()
  end

  @spec decode!(t()) :: map()
  def decode!(message) do
    message.data |> Poison.decode!()
  end
end
