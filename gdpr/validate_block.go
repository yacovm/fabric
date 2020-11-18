/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gdpr

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
)

func hash(preimage []byte) []byte {
	h := sha256.New()
	h.Write(preimage)
	return h.Sum(nil)
}

type Set map[string][]byte

func (s Set) Exists(element string) bool {
	_, ok := s[element]
	return ok
}

func (s Set) Lookup(hashedWrite []byte) []byte {
	val := s[string(hashedWrite)]
	return val
}

func HashedPreImages(preImages [][]byte) Set {
	hashesOfPreimages := make(Set)
	for _, preimage := range preImages {
		hashesOfPreimages[(string(hash(preimage)))] = preimage
	}
	return hashesOfPreimages
}

type TransactionValidation struct {
	err                   atomic.Value
	hashesInPreImageSpace Set
}

func (tv *TransactionValidation) validateHashesOfTxRWS(_ *common.Block, _ int, rws *rwsetutil.TxRwSet) {
	for _, nsrws := range rws.NsRwSets {
		for _, kvWrite := range nsrws.KvRwSet.Writes {
			fmt.Println("validateHashesOfTxRWS:", kvWrite.Key, kvWrite.Value)
			if !tv.hashesInPreImageSpace.Exists(string(kvWrite.ValueHash)) {
				err := fmt.Errorf("key %s write wasn't found in pre-image space", kvWrite.Key)
				tv.err.Store(err)
			} else {
				fmt.Println("key", kvWrite.Key, "write was found in pre-image space")
			}
		}
	}
}

func (tv *TransactionValidation) onErr(err error) {
	tv.err.Store(err)
}

func (tv *TransactionValidation) error() error {
	errVal := tv.err.Load()
	if errVal == nil {
		return nil
	}
	return errVal.(error)
}

func Validate(block *common.Block) error {
	txValidation := TransactionValidation{
		hashesInPreImageSpace: HashedPreImages(block.Data.PreimageSpace),
	}

	forEachTransaction(block, txValidation.validateHashesOfTxRWS, txValidation.onErr)

	return txValidation.error()
}

func forEachTransaction(block *common.Block, f func(block *common.Block, i int, rws *rwsetutil.TxRwSet), onErr func(err error)) {
	var workers sync.WaitGroup
	workers.Add(len(block.Data.Data))

	for i, envBytes := range block.Data.Data {
		go func(i int, envBytes []byte) {
			defer workers.Done()

			originalEnvelope, err := protoutil.GetEnvelopeFromBlock(envBytes)
			if err != nil {
				onErr(err)
				return
			}
			ProcessEnvelope(originalEnvelope, block, i, f, onErr)
		}(i, envBytes)
	}

	workers.Wait()
}

func ProcessEnvelope(originalEnvelope *common.Envelope, block *common.Block, i int, f func(block *common.Block, i int, rws *rwsetutil.TxRwSet), onErr func(err error)) {
	payload, err := protoutil.UnmarshalPayload(originalEnvelope.Payload)
	if err != nil {
		onErr(err)
		return
	}

	chdr, err := protoutil.ChannelHeader(originalEnvelope)
	if err != nil {
		onErr(err)
		return
	}

	if common.HeaderType(chdr.Type) != common.HeaderType_ENDORSER_TRANSACTION {
		return
	}

	tx, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		onErr(err)
		return
	}

	ccPayload, err := protoutil.UnmarshalChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		onErr(err)
		return
	}

	if ccPayload.Action == nil || ccPayload.Action.ProposalResponsePayload == nil {
		onErr(fmt.Errorf("no payload in ChaincodeActionPayload"))
	}
	pRespPayload, err := protoutil.UnmarshalProposalResponsePayload(ccPayload.Action.ProposalResponsePayload)
	if err != nil {
		onErr(err)
		return
	}

	if pRespPayload.Extension == nil {
		onErr(errors.New("response payload is missing extension"))
	}

	ccAction, err := protoutil.UnmarshalChaincodeAction(pRespPayload.Extension)
	if err != nil {
		onErr(err)
		return
	}

	txRWSet := &rwsetutil.TxRwSet{}

	if err = txRWSet.FromProtoBytes(ccAction.Results); err != nil {
		onErr(err)
		return
	}

	f(block, i, txRWSet)
}

func FoldReadWriteSet(block *common.Block, i int, txRWSet *rwsetutil.TxRwSet) {
	env, err := protoutil.UnmarshalEnvelope(block.Data.Data[i])
	if err != nil {
		panic(err)
	}

	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		panic(err)
	}

	chdr, err := protoutil.ChannelHeader(env)
	if err != nil {
		panic(err)
	}

	if common.HeaderType(chdr.Type) != common.HeaderType_ENDORSER_TRANSACTION {
		return
	}

	tx, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		panic(err)
	}

	ccPayload, err := protoutil.UnmarshalChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		panic(err)
	}

	pRespPayload, err := protoutil.UnmarshalProposalResponsePayload(ccPayload.Action.ProposalResponsePayload)
	if err != nil {
		panic(err)
	}

	ccAction, err := protoutil.UnmarshalChaincodeAction(pRespPayload.Extension)
	if err != nil {
		panic(err)
	}

	txRWSetBytes, err := txRWSet.ToProtoBytes()
	if err != nil {
		panic(err)
	}

	ccAction.Results = txRWSetBytes
	ccActionBytes, err := proto.Marshal(ccAction)
	if err != nil {
		panic(err)
	}

	pRespPayload.Extension = ccActionBytes

	ccPayload.Action.ProposalResponsePayload, err = proto.Marshal(pRespPayload)
	if err != nil {
		panic(err)
	}

	tx.Actions[0].Payload, err = proto.Marshal(ccPayload)
	if err != nil {
		panic(err)
	}

	payload.Data, err = proto.Marshal(tx)
	if err != nil {
		panic(err)
	}

	env.Payload, err = proto.Marshal(payload)
	if err != nil {
		panic(err)
	}

	envBytes, err := proto.Marshal(env)
	if err != nil {
		panic(err)
	}

	block.Data.Data[i] = envBytes
}
