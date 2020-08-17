/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gdpr

import (
	"errors"
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

	hashesOfPreimages := map[string]struct{}{}

	for _, im := range preImages {
		temp := getHash(im)
		hashesOfPreimages[(string(temp))] = struct{}{}
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
				var hashVal = getHash(kvWrite.GetValue()) // TODO: should be kvWrite.GetValueHash() when there are actual values
				//var hashVal = kvWrite.GetValueHash()
				if memberOf((string)(hashVal), hashesOfPreimages) == false {
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

	preImages := block.GetData().GetPreimageSpace()

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
				temp := findKVWrite(preImages, a)
				if temp != nil {
					kvWrite.Value = temp
				}
			}

		}
	}

	return newBlock, nil

}

func findKVWrite(preimages [][]byte, hashVal []byte) []byte {
	strVal := (string)(hashVal)
	for _, pi := range preimages {
		temp := (string)(getHash(pi))
		if temp == strVal {
			return pi
		}
	}
	return nil
}




