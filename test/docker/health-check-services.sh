#!/bin/bash

# $SECONDS contains the number of seconds elapsed since starting the script.
END_SECONDS=$((SECONDS+120))

for name in $(docker ps --format '{{.Names}}'); do
  while [[ "$SECONDS" -lt "$END_SECONDS" ]]; do
    STATUS=$(docker inspect --format "{{json .State.Health.Status }}" "$name")

    if [[ $STATUS == "\"healthy\"" ]]; then
      echo "Docker $name is up and running"
      break
    fi

    >&2 echo "Docker $name is unavailable, waiting to start...";
    sleep 3;
  done
done
