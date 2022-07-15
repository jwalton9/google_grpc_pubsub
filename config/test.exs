import Config

config :google_grpc_pubsub,
  start_pool: false,
  client: Google.Pubsub.Testing.Client

config :goth,
  disabled: true
