#!/usr/bin/env bash
source .elastic-version

docker-compose down -v
docker-compose -f metricstore-docker-compose.yml down -v
