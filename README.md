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

By default there can be 10 concurrent calls to the rpc channel, if you would like to increase this

```elixir
config :google_grpc_pubsub,
  pool_size: 50
```

## Getting Started

### Creating a topic

```elixir
{:ok, topic} = Pubsub.Topic.create(project: "my-project", topic: "my-topic")
```

### Create a subscription

```elixir
{:ok, subscription} = Pubsub.Subscription.create(project: "my-project", subscription: "my-subscription", topic: "my-topic")
```

### Publishing a message

```elixir
{:ok, topic} = Pubsub.Topic.get(project: "my-project", topic: "my-topic")

# Publish some string data
Pubsub.publish(topic, "my string data")

# Or you can publish a map, which will be encoded to JSON
Pubsub.publish(topic, %{some: "json data"})


# You can also publish multiple messages at once
Pubsub.publish(topic, [%{some: "json data"}, %{another: "message"}])
```

### Pulling Messages

```elixir
{:ok, subscription} = Pubsub.Subscription.get(project: "project", subscription: "subscription")

{:ok, messages} = Pubsub.Subscriber.pull(subscription, max_messages: 5)
```

### Acknowledging Messages

```elixir
{:ok, subscription} = Pubsub.Subscription.get(project: "project", subscription: "subscription")

{:ok, messages} = Pubsub.Subscriber.pull(subscription, max_messages: 5)

messages
|> Enum.map(&Pubsub.Message.decode!/1)
|> Enum.filter(fn message -> message.data["valid"] end)
|> Pubsub.acknowledge(subscription)
```
