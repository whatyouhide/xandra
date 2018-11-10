#!/bin/bash

for name in $(docker ps --format '{{.Names}}'); do
	while true; do
		STATUS=$(docker inspect --format "{{json .State.Health.Status }}" $name)
		if [[ $STATUS == "\"healthy\"" ]]; then
			echo "Docker $name is up and running($STATUS)"
			break
		fi
		>&2 echo "Docker $name is unavailable, waiting to start...";
		sleep 3;
	done
done
