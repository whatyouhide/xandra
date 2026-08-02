#!/bin/bash

# $SECONDS contains the number of seconds elapsed since starting the script.
MAX_SECONDS=120
END_SECONDS=$((SECONDS+MAX_SECONDS))

CONTAINER_IDS="$(docker compose ps --all --quiet)"

if [[ -z "$CONTAINER_IDS" ]]; then
  >&2 echo "Docker Compose did not create any containers"
  exit 1
fi

while [[ "$SECONDS" -lt "$END_SECONDS" ]]; do
  ALL_HEALTHY=true

  for container_id in $CONTAINER_IDS; do
    name="$(docker inspect --format '{{.Name}}' "$container_id")"

    if [[ "$name" == *"toxiproxy"* ]]; then
      continue
    fi

    state="$(docker inspect --format '{{.State.Status}}' "$container_id")"

    if [[ "$state" != "running" ]]; then
      >&2 echo "Docker container '$name' exited while starting (state: $state)"
      exit 1
    fi

    health="$(docker inspect --format '{{.State.Health.Status}}' "$container_id")"

    if [[ "$health" != "healthy" ]]; then
      >&2 echo "Docker container '$name' is unavailable, waiting to start..."
      ALL_HEALTHY=false
    fi
  done

  if [[ "$ALL_HEALTHY" == true ]]; then
    for container_id in $CONTAINER_IDS; do
      name="$(docker inspect --format '{{.Name}}' "$container_id")"

      if [[ "$name" != *"toxiproxy"* ]]; then
        echo "Docker container '$name' is up and running"
      fi
    done

    exit 0
  fi

  sleep 3
done

for container_id in $CONTAINER_IDS; do
  name="$(docker inspect --format '{{.Name}}' "$container_id")"

  if [[ "$name" != *"toxiproxy"* ]]; then
    state="$(docker inspect --format '{{.State.Status}}' "$container_id")"
    health="$(docker inspect --format '{{.State.Health.Status}}' "$container_id")"
    >&2 echo "Docker container '$name' failed to start (state: $state, health: $health)"
  fi
done

exit 1
