#!/bin/bash

# $SECONDS contains the number of seconds elapsed since starting the script.
MAX_SECONDS=120
END_SECONDS=$((SECONDS+MAX_SECONDS))

for name in $(docker ps --format '{{.Names}}'); do
  HEALTHY=false

  while [[ "$SECONDS" -lt "$END_SECONDS" ]]; do
    STATUS="$(docker inspect --format "{{json .State.Health.Status }}" "$name")"

    if [[ "$STATUS" == '"healthy"' ]]; then
      echo "Docker container '$name' is up and running"
      HEALTHY=true
      break
    fi

    >&2 echo "Docker container '$name' is unavailable, waiting to start...";
    sleep 3;
  done

  if [[ "$HEALTHY" == false ]]; then
    >&2 echo "Docker container '$name' failed to start after $MAX_SECONDS seconds"
    exit 1
  fi
done
