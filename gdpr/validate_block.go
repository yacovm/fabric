/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gdpr

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"

	"crypto/sha256"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
)

var ErrVal = errors.New("gdpr: block validation failed")

func getHash(preimage []byte) []byte {
	h := sha256.New()
	return h.Sum(preimage)

}

func validate(block *common.Block) (*common.Block, error) {
	preImages := block.GetData().GetPreimageSpace()

	m := map[string]struct{}{}

	for _, im := range preImages {
		temp := getHash(im)
		m[(string(temp))] = struct{}{}
	}

	for _, envBytes := range block.Data.Data {
		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			return nil, err
		}

		payload, err := protoutil.UnmarshalPayload(env.Payload)
		if err != nil {
			return nil, err
		}

		tx, err := protoutil.UnmarshalTransaction(payload.Data)
		if err != nil {
			return nil, err
		}

		_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
		if err != nil {
			return nil, err
		}

		txRWSet := &rwsetutil.TxRwSet{}

		if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
			return nil, err
		}

		for _, nsRWSet := range txRWSet.NsRwSets {
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				var a = kvWrite.GetValueHash()
				if a != nil {
					fmt.Printf("that's odd")
				}
				var b = getHash(kvWrite.GetValue())
				if memberOf((string)(b), m) == false {
					return nil, ErrVal
				}

			}

		}
	}

	return getVanillaBlock(block)
}

func memberOf(a string, m map[string]struct{}) bool {
	_, exists := m[a]
	return exists

}

func getVanillaBlock(block *common.Block) (*common.Block, error) {
	newBlock := proto.Clone(block).(*common.Block)
	newBlock.Data.PreimageSpace = nil

	clearKVWrites(newBlock) // TODO: remove when blocks are generated without KVWrite values.
	//preImages := block.GetData().GetPreimageSpace() // TODO: to be used in real world
	preImages := extractPreimages(block)

	for _, envBytes := range newBlock.Data.Data {
		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			return nil, err
		}

		payload, err := protoutil.UnmarshalPayload(env.Payload)
		if err != nil {
			return nil, err
		}

		tx, err := protoutil.UnmarshalTransaction(payload.Data)
		if err != nil {
			return nil, err
		}

		_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
		if err != nil {
			return nil, err
		}

		txRWSet := &rwsetutil.TxRwSet{}

		if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
			return nil, err
		}
		for _, nsRWSet := range txRWSet.NsRwSets {
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				var a = kvWrite.GetValueHash()
				temp := getKVWrite(preImages, a)
				if temp != nil {
					kvWrite.Value = temp
				}
			}

		}
	}

	return newBlock, nil

}

func getKVWrite(preimages [][]byte, hashVal []byte) []byte {
	strVal := (string)(hashVal)
	for _, pi := range preimages {
		temp := (string)(getHash(pi))
		if temp == strVal {
			return pi
		}
	}
	return nil
}

func extractPreimages(block *common.Block) [][]byte {

	var preimages [][]byte

	for _, envBytes := range block.Data.Data {
		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			continue
		}

		payload, err := protoutil.UnmarshalPayload(env.Payload)
		if err != nil {
			continue
		}

		tx, err := protoutil.UnmarshalTransaction(payload.Data)
		if err != nil {
			return nil
		}

		_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
		if err != nil {
			return nil
		}

		txRWSet := &rwsetutil.TxRwSet{}

		if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
			return nil
		}
		for _, nsRWSet := range txRWSet.NsRwSets {
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				var a = kvWrite.GetValue()
				preimages = append(preimages, a)
			}

		}

	}

	return preimages
}

func clearKVWrites(block *common.Block) {
	for _, envBytes := range block.Data.Data {
		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			return
		}

		payload, err := protoutil.UnmarshalPayload(env.Payload)
		if err != nil {
			return
		}

		tx, err := protoutil.UnmarshalTransaction(payload.Data)
		if err != nil {
			return
		}

		_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
		if err != nil {
			return
		}

		txRWSet := &rwsetutil.TxRwSet{}

		if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
			return
		}
		for _, nsRWSet := range txRWSet.NsRwSets {
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				kvWrite.Value = nil
			}
		}
	}

}
