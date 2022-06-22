ARG ELIXIR_VERSION=1.13.4
ARG OTP_VERSION=24.2

FROM hexpm/elixir:${ELIXIR_VERSION}-erlang-${OTP_VERSION}-alpine-3.14.2

WORKDIR /app

ENV MIX_ENV=test

# Install Docker and Docker Compose to control sibling containers, and Git for installing
# Git dependencies if necessary.
RUN apk update && \
    apk add docker docker-compose git curl openssl

# Copy only the files needed to fetch and compile deps.
COPY mix.exs .
COPY mix.lock .

# Install rebar3 and Hex and then compile dependencies. This will be cached by
# Docker unless we change dependencies.
RUN mix do local.rebar --force, \
    local.hex --force, \
    deps.get --only test, \
    deps.compile

# Now copy Xandra's code.
COPY lib lib
COPY test test
COPY test_clustering test_clustering
