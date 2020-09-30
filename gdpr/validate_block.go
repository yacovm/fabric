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

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
)

func hash(preimage []byte) []byte {
	h := sha256.New()
	h.Write(preimage)
	return h.Sum(nil)
}

type set map[string]struct{}

func (s set) exists(element string) bool {
	_, ok := s[element]
	return ok
}

func hashedPreImages(block *common.Block) set {
	hashesOfPreimages := make(set)
	for _, preimage := range block.GetData().GetPreimageSpace() {
		hashesOfPreimages[(string(hash(preimage)))] = struct{}{}
	}
	return hashesOfPreimages
}

type transactionValidation struct {
	err                   atomic.Value
	hashesInPreImageSpace set
}

func (tv *transactionValidation) validateHashesOfTxRWS(_ *common.Block, _ int, rws *rwsetutil.TxRwSet) {
	for _, nsrws := range rws.NsRwSets {
		for _, kvWrite := range nsrws.KvRwSet.Writes {
			fmt.Println("validateHashesOfTxRWS:", kvWrite.Key, kvWrite.Value)
			if !tv.hashesInPreImageSpace.exists(string(kvWrite.ValueHash)) {
				err := fmt.Errorf("key %s write wasn't found in pre-image space", kvWrite.Key)
				tv.err.Store(err)
			} else {
				fmt.Println("key", kvWrite.Key, "write was found in pre-image space")
			}
		}
	}
}

func (tv *transactionValidation) onErr(err error) {
	tv.err.Store(err)
}

func (tv *transactionValidation) error() error {
	errVal := tv.err.Load()
	if errVal == nil {
		return nil
	}
	return errVal.(error)
}

func Validate(block *common.Block) error {
	txValidation := transactionValidation{
		hashesInPreImageSpace: hashedPreImages(block),
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
			ProcessEnvelope(envBytes, block, i, f, onErr)
		}(i, envBytes)
	}

	workers.Wait()
}

func ProcessEnvelope(envBytes []byte, block *common.Block, i int, f func(block *common.Block, i int, rws *rwsetutil.TxRwSet), onErr func(err error)) {
	originalEnvelope, err := protoutil.GetEnvelopeFromBlock(envBytes)
	if err != nil {
		onErr(err)
		return
	}

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
