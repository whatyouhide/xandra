#!/bin/bash -e

docker-compose -f docker-compose.cluster.yml build elixir

docker-compose -f docker-compose.cluster.yml run --rm elixir &
TEST_PID=$!

echo "==> Sleeping..."
sleep 10

echo "==> Stopping C* node"
docker-compose -f docker-compose.cluster.yml stop node1

wait "$TEST_PID"

docker-compose -f docker-compose.cluster.yml up -d
