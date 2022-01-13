/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"os"

	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/cmd/common"
)

func main() {
	factory.InitFactories(nil)

	cli := common.NewCLI("bench", "Command line tool to benchmark fabric orderers with committee selection")

	b := Benchmark{}

	cmd := cli.Command("committee", "send generated traffic to the committee", b.Run)
	tps := cmd.Flag("TPS", "number of transactions per second to be sent by each worker").Int()
	workerNum := cmd.Flag("workerNum", "number of workers that send transactions in parallel").Int()
	channel := cmd.Flag("channel", "name of the channel the transactions are directed to").String()
	endpoint := cmd.Flag("endpoint", "endpoint (host:port) of some orderer node").String()
	verbose := cmd.Flag("verbose", "print debug information").Bool()

	b.TPS = tps
	b.WorkerNum = workerNum
	b.Channel = channel
	b.Endpoint = endpoint
	b.Verbose = verbose

	cli.Run(os.Args[1:])
}
