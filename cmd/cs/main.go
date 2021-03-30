/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/hyperledger/fabric/orderer/consensus/smartbft"

	cs "github.com/SmartBFT-Go/randomcommittees"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/cmd/common"
	"github.com/hyperledger/fabric/common/flogging"
)

var logger = flogging.MustGetLogger("selection.cli")

func sha256Hash(bytes []byte) []byte {
	h := sha256.New()
	h.Write(bytes)
	return h.Sum(nil)
}

func main() {
	factory.InitFactories(nil)

	cli := common.NewCLI("cs", "Command line tool to detect and print public key for committee selection")
	cli.Command("detect", "detects public key", func(config common.Config) error {
		keyFilePath := config.SignerConfig.KeyPath
		_, err := os.Stat(keyFilePath)
		if os.IsNotExist(err) {
			return fmt.Errorf("key files %s, doesn't exists", keyFilePath)
		}

		privateKeyBytes, err := ioutil.ReadFile(keyFilePath)
		if err != nil {
			return err
		}

		rndSeed := smartbft.NewPRG(sha256Hash(privateKeyBytes))

		selection := cs.NewCommitteeSelection(logger)
		pk, _, err := selection.GenerateKeyPair(rndSeed)
		if err != nil {
			return err
		}

		fmt.Println(base64.StdEncoding.EncodeToString(pk))
		return nil
	})
	cli.Run(os.Args[1:])
}
