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

	cs "github.com/SmartBFT-Go/randomcommittees"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/cmd/common"
	"github.com/hyperledger/fabric/common/flogging"
)

var logger = flogging.MustGetLogger("selection.cli")

type rnd struct {
	seed   []byte
	buffer []byte
}

func (r *rnd) Read(p []byte) (n int, err error) {
	for len(r.buffer) < len(p) {
		r.buffer = append(r.buffer, sha256Hash(r.seed)...)
		r.seed = sha256Hash(r.seed)
	}

	n = copy(p, r.buffer)
	r.buffer = r.buffer[n:]
	return n, nil
}

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

		rndSeed := &rnd{
			seed: sha256Hash(privateKeyBytes),
		}

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
