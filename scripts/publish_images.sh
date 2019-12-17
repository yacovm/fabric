#!/usr/bin/env bash -e

echo "Building docker images..."
echo "DOCKER_PASSWORD=${DOCKER_PASSWORD}"
sleep 5
git checkout origin/release-1.4-BFT

make orderer-docker peer-docker

echo "Tagging images..."
docker tag hyperledger/fabric-peer:latest smartbft/fabric-peer:latest
docker tag hyperledger/fabric-orderer:latest smartbft/fabric-orderer:latest

echo "Logging in..."
docker login -u smartbft -p ${DOCKER_PASSWORD}

echo "Pushing to dockerhub"

docker push smartbft/fabric-peer:latest
docker push smartbft/fabric-orderer:latest

