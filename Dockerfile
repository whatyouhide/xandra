FROM elixir:1.13.2

WORKDIR /app
ENV MIX_ENV=test LOG_LEVEL=debug

COPY mix.exs .
COPY mix.lock .

RUN mix do local.rebar --force, local.hex --force, deps.get, deps.compile

COPY lib lib
COPY test test
COPY test.sh .
COPY cc_test.exs .

CMD ["mix", "run", "cc_test.exs"]
