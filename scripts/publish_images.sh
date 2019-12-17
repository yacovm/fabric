#!/usr/bin/env bash -e

echo "Building docker images..."
git checkout origin/release-1.4-BFT

make orderer-docker peer-docker

