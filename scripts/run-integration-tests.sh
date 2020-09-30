#!/bin/bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

# Use ginkgo to run integration tests. If arguments are provided to the
# script, they are treated as the directories containing the tests to run.
# When no arguments are provided, all integration tests are executed.

set -eu


echo "Pulling docker images"
make docker-thirdparty

make docker

cd integration/lifecycle

echo "Installing Ginkgo :( "
go get github.com/onsi/ginkgo/ginkgo
go get github.com/onsi/gomega/...

echo "running integration test"
/Users/galassa/gopath//bin/ginkgo --focus "deploys and executes chaincode using _lifecycle and upgrades it"
