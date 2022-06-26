defmodule Pubsub.Topic do
  alias Pubsub.Client
  alias Google.Pubsub.V1.Publisher.Stub

  @type t :: %__MODULE__{
          id: String.t()
        }

  defstruct [:id]

  @spec new(project: String.t(), name: String.t()) :: t()
  def new(opts) do
    %__MODULE__{id: id(opts[:project], opts[:name])}
  end

  @spec create(t()) :: {:ok, Google.Pubsub.V1.Topic.t()} | {:error, any()}
  def create(struct) do
    topic = Google.Pubsub.V1.Topic.new(name: struct.id)

    Client.call(Stub, :create_topic, topic)
  end

  @spec id(String.t(), String.t()) :: String.t()
  defp id(project, name) do
    Path.join(["projects", project, "topics", name])
  end
end
