defmodule Xandra.Authenticator do
  @callback response_body(Keyword.t) :: iodata
end
