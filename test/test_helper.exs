Mox.defmock(Google.Pubsub.ClientMock, for: Google.Pubsub.Client)

Application.put_env(:google_grpc_pubsub, :client, Google.Pubsub.ClientMock)

ExUnit.start()
