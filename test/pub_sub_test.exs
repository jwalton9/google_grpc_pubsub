defmodule Google.PubSubTest do
  use ExUnit.Case
  doctest Google.Pubsub

  test "greets the world" do
    assert Google.Pubsub.hello() == :world
  end
end
