# Pubsub

Elixir Library for interacting with Google Pubsub over GRPC, inspired by [Weddell](https://github.com/cjab/weddell)

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `google_grpc_pubsub` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:google_grpc_pubsub, "~> 0.1.0"}
  ]
end
```

## Configuration

`google_grpc_pubsub` uses `Goth` to authenticate with Google's APIs.

If you want to use this library against the Pubsub emulator

```elixir
config :google_grpc_pubsub,
  emulator: {"localhost", 8085}

config :goth,
  disabled: true
```

## Getting Started

### Creating a topic and subscription

```elixir
project = "my-project"
topic =
  Pubsub.Topic.new(project: project, name: "my-topic")
  |> Pubsub.Topic.create()

Pubsub.Subscription.new(project: project, name: "my-subscription")
|> Pubsub.Subscription.create(topic)
```

### Publishing a message

```elixir
alias Pubsub.{Topic, Publisher}

topic = Topic.new(project: "project", name: "topic")

# Publish some string data
Publisher.publish(topic, "my string data")

# Or you can publish a map, which will be encoded to JSON
Publisher.publish(topic, %{some: "json data"})


# You can also publish multiple messages at once
Publisher.publish(topic, [%{some: "json data"}, %{another: "message"}])
```

### Pulling Messages

```elixir
alias Pubsub.{Subscription, Subscriber}

subscription = Subscription.new(project: "project", name: "subscription")

case Subscriber.pull(subscription, max_messages: 5) do
  {:ok, messages} ->
    IO.puts("Received #{length(messages)} messages")

  {:error, error} ->
    raise error
end
```

### Acknowledging Messages

```elixir
alias Pubsub.{Message, Subscriber}

case Subscriber.pull(subscription, max_messages: 5) do
  {:ok, messages} ->
    messages = Enum.map(messages, fn message ->
      case message.data do
        "valid" -> Message.ack(message)
        _ -> message
      end
    end)

    Subscriber.acknowledge(subscription, messages)

  {:error, error} ->
    {:error, error}
end
```
