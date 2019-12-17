#!/usr/bin/env bash -e

echo "Building docker images..."
echo "DOCKER_PASSWORD=${DOCKER_PASSWORD}"
sleep 5
git checkout origin/release-1.4-BFT

make orderer-docker peer-docker

