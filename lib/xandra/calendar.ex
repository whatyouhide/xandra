# Copyright 2012 Plataformatec
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Modifications copyright 2017 Aleksei Magusev
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

defmodule Xandra.Calendar do
  # NOTE: Time.add/3 was introduced in Elixir v1.6.
  if Code.ensure_loaded?(Time) and function_exported?(Time, :add, 3) do
    def time_from_nanoseconds(nanoseconds) do
      Time.add(~T[00:00:00], nanoseconds, :nanosecond)
    end
  else
    def time_from_nanoseconds(nanoseconds) do
      nanoseconds
      |> DateTime.from_unix!(:nanoseconds)
      |> DateTime.to_time()
    end
  end

  # NOTE: Time.diff/3, Date.add/2, and Date.diff/2 were introduced in Elixir v1.5.
  if Code.ensure_loaded?(Time) and function_exported?(Time, :diff, 3) and
       Code.ensure_loaded?(Date) and function_exported?(Date, :add, 2) and
       function_exported?(Date, :diff, 2) do
    def time_to_nanoseconds(time) do
      Time.diff(time, ~T[00:00:00.000000], :nanosecond)
    end

    def date_from_unix_days(days) do
      Date.add(~D[1970-01-01], days)
    end

    def date_to_unix_days(date) do
      Date.diff(date, ~D[1970-01-01])
    end
  else
    @unix_epoch_days 719_528

    @seconds_per_minute 60
    @seconds_per_hour 60 * 60
    # Note that this does _not_ handle leap seconds.
    @seconds_per_day 24 * 60 * 60
    @microseconds_per_second 1_000_000
    @parts_per_day @seconds_per_day * @microseconds_per_second

    @days_per_nonleap_year 365
    @days_per_leap_year 366

    def time_to_nanoseconds(time) do
      %{hour: hour, minute: minute, second: second, microsecond: microsecond} = time
      {parts, ppd} = time_to_day_fraction(hour, minute, second, microsecond)
      microseconds = div(parts * @parts_per_day, ppd)
      System.convert_time_unit(microseconds, :microseconds, :nanoseconds)
    end

    def date_from_unix_days(days) do
      {year, month, day} = date_from_iso_days(days + @unix_epoch_days)
      %Date{year: year, month: month, day: day, calendar: Calendar.ISO}
    end

    def date_to_unix_days(%{calendar: Calendar.ISO} = date) do
      %{year: year, month: month, day: day} = date
      date_to_iso_days(year, month, day) - @unix_epoch_days
    end

    defp time_to_day_fraction(hour, minute, second, {microsecond, _}) do
      combined_seconds = hour * @seconds_per_hour + minute * @seconds_per_minute + second
      {combined_seconds * @microseconds_per_second + microsecond, @parts_per_day}
    end

    defp date_to_iso_days(year, month, day) when year in 0..9999 do
      true = day <= days_in_month(year, month)

      days_in_previous_years(year) + days_before_month(month) + leap_day_offset(year, month) + day -
        1
    end

    defp days_in_month(year, 2) do
      if Calendar.ISO.leap_year?(year), do: 29, else: 28
    end

    defp days_in_month(_, month) when month in [4, 6, 9, 11], do: 30
    defp days_in_month(_, month) when month in 1..12, do: 31

    defp days_before_month(1), do: 0
    defp days_before_month(2), do: 31
    defp days_before_month(3), do: 59
    defp days_before_month(4), do: 90
    defp days_before_month(5), do: 120
    defp days_before_month(6), do: 151
    defp days_before_month(7), do: 181
    defp days_before_month(8), do: 212
    defp days_before_month(9), do: 243
    defp days_before_month(10), do: 273
    defp days_before_month(11), do: 304
    defp days_before_month(12), do: 334

    defp leap_day_offset(_year, month) when month < 3, do: 0

    defp leap_day_offset(year, _month) do
      if Calendar.ISO.leap_year?(year), do: 1, else: 0
    end

    defp date_from_iso_days(days) when days in 0..3_652_424 do
      {year, day_of_year} = days_to_year(days)
      extra_day = if Calendar.ISO.leap_year?(year), do: 1, else: 0
      {month, day_in_month} = year_day_to_year_date(extra_day, day_of_year)
      {year, month, day_in_month + 1}
    end

    defp days_to_year(days) do
      year = floor_div(days, @days_per_nonleap_year)
      {year, days_before_year} = days_to_year(year, days, days_in_previous_years(year))
      {year, days - days_before_year}
    end

    defp days_to_year(year, days1, days2) when days1 < days2 do
      days_to_year(year - 1, days1, days_in_previous_years(year - 1))
    end

    defp days_to_year(year, _days1, days2) do
      {year, days2}
    end

    defp days_in_previous_years(0), do: 0

    defp days_in_previous_years(year) do
      previous_year = year - 1

      floor_div(previous_year, 4) - floor_div(previous_year, 100) + floor_div(previous_year, 400) +
        previous_year * @days_per_nonleap_year + @days_per_leap_year
    end

    # Note that 0 is the first day of the month.
    defp year_day_to_year_date(_extra_day, day_of_year) when day_of_year < 31 do
      {1, day_of_year}
    end

    defp year_day_to_year_date(extra_day, day_of_year) when day_of_year < 59 + extra_day do
      {2, day_of_year - 31}
    end

    defp year_day_to_year_date(extra_day, day_of_year) when day_of_year < 90 + extra_day do
      {3, day_of_year - (59 + extra_day)}
    end

    defp year_day_to_year_date(extra_day, day_of_year) when day_of_year < 120 + extra_day do
      {4, day_of_year - (90 + extra_day)}
    end

    defp year_day_to_year_date(extra_day, day_of_year) when day_of_year < 151 + extra_day do
      {5, day_of_year - (120 + extra_day)}
    end

    defp year_day_to_year_date(extra_day, day_of_year) when day_of_year < 181 + extra_day do
      {6, day_of_year - (151 + extra_day)}
    end

    defp year_day_to_year_date(extra_day, day_of_year) when day_of_year < 212 + extra_day do
      {7, day_of_year - (181 + extra_day)}
    end

    defp year_day_to_year_date(extra_day, day_of_year) when day_of_year < 243 + extra_day do
      {8, day_of_year - (212 + extra_day)}
    end

    defp year_day_to_year_date(extra_day, day_of_year) when day_of_year < 273 + extra_day do
      {9, day_of_year - (243 + extra_day)}
    end

    defp year_day_to_year_date(extra_day, day_of_year) when day_of_year < 304 + extra_day do
      {10, day_of_year - (273 + extra_day)}
    end

    defp year_day_to_year_date(extra_day, day_of_year) when day_of_year < 334 + extra_day do
      {11, day_of_year - (304 + extra_day)}
    end

    defp year_day_to_year_date(extra_day, day_of_year) do
      {12, day_of_year - (334 + extra_day)}
    end

    defp floor_div(dividend, divisor) do
      if dividend * divisor < 0 and rem(dividend, divisor) != 0 do
        div(dividend, divisor) - 1
      else
        div(dividend, divisor)
      end
    end
  end
end
