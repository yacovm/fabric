#!/bin/bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

# Use ginkgo to run integration tests. If arguments are provided to the
# script, they are treated as the directories containing the tests to run.
# When no arguments are provided, all integration tests are executed.

set -eu

docker pull hyperledger/fabric-ccenv:latest
docker pull hyperledger/fabric-ccenv:2.2
docker pull hyperledger/fabric-ccenv:2.2.0

echo "Pulling docker images"
make docker-thirdparty

cd integration/lifecycle

echo "Installing Ginkgo :( "
go get github.com/onsi/ginkgo/ginkgo
go get github.com/onsi/gomega/...

echo "running integration test"
ginkgo --focus "deploys and executes chaincode using _lifecycle and upgrades it"
