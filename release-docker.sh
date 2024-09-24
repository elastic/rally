#!/usr/bin/env bash

# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Prerequisites for releasing:

# Logged in on Docker Hub (docker login)

# fail this script immediately if any command fails with a non-zero exit code
set -eu

function push_failed {
    echo "Error while pushing Docker image. Did you \`docker login\`?"
}

if [[ $# -eq 0 ]] ; then
    echo "ERROR: $0 requires the Rally version to push as a command line argument and you didn't supply it."
    echo "For example: $0 1.1.0"
    exit 1
fi
export RALLY_VERSION=$1
export RALLY_LICENSE=$(awk 'FNR>=2 && FNR<=2' LICENSE | sed 's/^[ \t]*//')

echo "========================================================"
echo "Building Docker image for Rally release $RALLY_VERSION  "
echo "========================================================"

# Create builder that supports QEMU emulation (needed for linux/arm64)
docker buildx rm --force rally-multiarch-builder || true
docker buildx create --name rally-multiarch-builder --bootstrap --use
docker buildx build --load \
  --tag elastic/rally:${RALLY_VERSION} \
  --build-arg RALLY_VERSION --build-arg RALLY_LICENSE \
  --file docker/Dockerfiles/Dockerfile-release \
  --platform linux/amd64 \
  "$PWD"

echo "======================================================="
echo "Testing Docker image for Rally release $RALLY_VERSION  "
echo "======================================================="

./release-docker-test.sh

echo "======================="
echo "Publishing Docker image"
echo "======================="

trap push_failed ERR

# Multi-arch images have to be pushed immediately forcing us to rebuild here to
# add arm64 - https://github.com/docker/buildx/issues/59
docker buildx build \
  --push \
  --tag elastic/rally:${RALLY_VERSION} \
  --tag elastic/rally:latest \
  --build-arg RALLY_VERSION --build-arg RALLY_LICENSE \
  --file docker/Dockerfiles/Dockerfile-release \
  --platform linux/amd64,linux/arm64 \
  "$PWD"

trap - ERR
