# Google.Pubsub

Elixir Library for interacting with Google Pubsub over GRPC, inspired by [Weddell](https://github.com/cjab/weddell)

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `google_grpc_pubsub` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:google_grpc_pubsub, "~> 0.1.2"}
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
{:ok, topic} = Google.Pubsub.Topic.create(project: "my-project", topic: "my-topic")
```

### Create a subscription

```elixir
{:ok, subscription} = Google.Pubsub.Subscription.create(project: "my-project", subscription: "my-subscription", topic: "my-topic")
```

### Publishing a message

```elixir
{:ok, topic} = Google.Pubsub.Topic.get(project: "my-project", topic: "my-topic")

# Publish some string data
Google.PubSubT.Topic.publish(topic, "my string data")

# Or you can publish a map, which will be encoded to JSON
Google.Pubsub.Topic.publish(topic, %{some: "json data"})


# You can also publish multiple messages at once
Google.Pubsub.Topic.publish(topic, [%{some: "json data"}, %{another: "message"}])
```

### Pulling Messages

```elixir
{:ok, subscription} = Google.Pubsub.Subscription.get(project: "project", subscription: "subscription")

{:ok, messages} = Google.Pubsub.Subscription.pull(subscription, max_messages: 5)
```

### Acknowledging Messages

```elixir
{:ok, subscription} = Google.Pubsub.Subscription.get(project: "project", subscription: "subscription")

{:ok, messages} = Google.Pubsub.Subscription.pull(subscription, max_messages: 5)

messages
|> Enum.map(&Google.Pubsub.Message.decode!/1)
|> Enum.filter(fn message -> message.data["valid"] end)
|> Pubsub.acknowledge(subscription)
```
