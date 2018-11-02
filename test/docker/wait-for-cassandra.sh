#!/bin/bash
set -e

# Max query attempts before consider setup failed
MAX_TRIES=12

function cassandra-is-ready() {
  docker-compose logs cassandra | grep "Starting listening for CQL clients" > /dev/null
}

function wait-until-cassandra-is-ready() {
  attempt=1

  while [ $attempt -le $MAX_TRIES ]; do
    if cassandra-is-ready; then
      echo "Cassandra is up and running"
      break
    fi

    sleep 3
  done

  if [ $attempt -gt $MAX_TRIES ]; then
    echo "Cassandra not responding, cancelling set up"
    exit 1
  fi
}

wait-until-cassandra-is-ready
