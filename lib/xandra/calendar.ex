defmodule Xandra.Calendar do
  @moduledoc false

  def time_from_nanoseconds(nanoseconds) do
    Time.add(~T[00:00:00], nanoseconds, :nanosecond)
  end

  def time_to_nanoseconds(time) do
    Time.diff(time, ~T[00:00:00.000000], :nanosecond)
  end

  def date_from_unix_days(days) do
    Date.add(~D[1970-01-01], days)
  end

  def date_to_unix_days(date) do
    Date.diff(date, ~D[1970-01-01])
  end
end
