defmodule Google.Protobuf.FieldMask do
  @moduledoc false
  use Protobuf, syntax: :proto3

  field(:paths, 1, repeated: true, type: :string)
end
