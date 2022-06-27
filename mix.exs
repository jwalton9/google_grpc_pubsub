defmodule Pubsub.MixProject do
  use Mix.Project

  def project do
    [
      app: :google_grpc_pubsub,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      source_url: "https://github.com/jwalton9/google_grpc_pubsub",
      description: "Elixir Library for interacting with Google Pubsub over GRPC",
      docs: [
        main: "readme",
        extras: ["README.md"]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Pubsub, []},
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:certifi, "~> 2.9"},
      {:cowlib, "~> 2.9", override: true},
      {:dialyxir, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, "~> 0.27", only: :dev, runtime: false},
      {:goth, "~> 1.2"},
      {:grpc, "~> 0.3"},
      {:poison, "~> 5.0"},
      {:poolboy, "~> 1.5.1"},
      {:protobuf, "~> 0.10"}
    ]
  end
end
