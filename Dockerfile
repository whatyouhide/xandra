# Use an official Elixir image as a base
FROM elixir:1.15.0

# Install build tools, dependencies, and runtime packages
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    openssh-client \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV MIX_ENV=prod \
    LANG=C.UTF-8

# Create and set the working directory
WORKDIR /app

# Copy mix configuration files
COPY mix.exs mix.lock ./

# Fetch and compile dependencies
RUN mix local.hex --force && mix local.rebar --force && mix deps.get && mix deps.compile

# Copy the application source code
COPY . .

# Compile the application
RUN mix compile

# Expose the default port (adjust if needed)
EXPOSE 4000

# Run the application
CMD ["mix", "run", "--no-halt"]