defmodule Pubsub.Message do
  alias Google.Pubsub.V1.{ReceivedMessage, PubsubMessage}

  @type t :: %__MODULE__{
          ack_id: String.t(),
          data: String.t()
        }

  @type decoded_message :: %__MODULE__{
          ack_id: String.t(),
          data: map()
        }

  defstruct ack_id: nil, data: nil

  @spec new(ReceivedMessage.t()) :: t()
  def new(%ReceivedMessage{ack_id: ack_id, message: %PubsubMessage{data: data}}) do
    %__MODULE__{
      ack_id: ack_id,
      data: data
    }
  end

  @spec decode(t()) :: {:ok, decoded_message()} | {:error, any()}
  def decode(message) do
    case Poison.decode(message.data) do
      {:ok, data} ->
        {:ok, %{message | data: data}}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec decode!(t()) :: decoded_message()
  def decode!(message) do
    case decode(message) do
      {:ok, decoded_message} ->
        decoded_message

      {:error, error} ->
        raise error
    end
  end
end
