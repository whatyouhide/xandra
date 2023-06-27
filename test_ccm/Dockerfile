ARG ELIXIR_VERSION=1.14.1
ARG OTP_VERSION=25.1
FROM hexpm/elixir:${ELIXIR_VERSION}-erlang-${OTP_VERSION}-ubuntu-xenial-20210804

WORKDIR /app

ENV MIX_ENV=test

# Install Docker and Docker Compose to control sibling containers, and Git for installing
# Git dependencies if necessary.
RUN apt-get update && \
    apt-get install -y git openssl curl python3-pip python3-dev openjdk-8-jdk

RUN pip3 install ccm
RUN ccm create test -v 3.11.0 && ccm populate -n 4

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
COPY test_ccm test_ccm
