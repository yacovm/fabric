/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"

	cs "github.com/SmartBFT-Go/randomcommittees"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/cmd/common"
	"github.com/hyperledger/fabric/common/flogging"
)

var logger = flogging.MustGetLogger("selection.cli")

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

		h := sha256.New()
		h.Write(privateKeyBytes)
		digest := h.Sum(nil)

		rndSeed := bytes.NewBuffer(digest)

		selection := cs.NewCommitteeSelection(logger)
		pk, _, err := selection.GenerateKeyPair(rndSeed)
		if err != nil {
			return err
		}

		fmt.Printf("Public key for committee selection: \n %s\n", base64.StdEncoding.EncodeToString(pk))
		return nil
	})
	cli.Run(os.Args[1:])
}
