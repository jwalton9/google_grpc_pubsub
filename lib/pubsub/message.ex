defmodule Pubsub.Message do
  alias Google.Pubsub.V1.{ReceivedMessage, PubsubMessage}

  @type t :: %__MODULE__{
          ack_id: String.t(),
          data: any(),
          acked?: boolean()
        }

  defstruct [:ack_id, :data, :acked?]

  @spec new(ReceivedMessage.t()) :: t()
  def new(%ReceivedMessage{ack_id: ack_id, message: %PubsubMessage{data: data}}) do
    %__MODULE__{
      ack_id: ack_id,
      acked?: false,
      data: data
    }
  end

  @spec ack(t()) :: t()
  def ack(struct) do
    %__MODULE__{struct | acked?: true}
  end
end
