/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msgprocessor

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	ab "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
)

type redactionRule struct{}
type set map[string]struct{}


func (a redactionRule) Apply(message *ab.Envelope) error {
	if message.Payload == nil {
		return ErrEmptyMessage
	}

	hashesOfPreimages := make(set)
	for _, preimage := range message.PreImages {
		hashesOfPreimages[(string(util.ComputeSHA256(preimage)))] = struct{}{}
	}

	msgData := &ab.Payload{}

	err := proto.Unmarshal(message.Payload, msgData)
	if err != nil{
		return err
	}

	payload, err := protoutil.UnmarshalPayload(msgData.Data)
	if err != nil{
		return err
	}

	tx, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		return err
	}

	ccPayload, err := protoutil.UnmarshalChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		return err
	}

	if ccPayload.Action == nil || ccPayload.Action.ProposalResponsePayload == nil {
		return errors.New("no payload in ChaincodeActionPayload")
	}
	pRespPayload, err := protoutil.UnmarshalProposalResponsePayload(ccPayload.Action.ProposalResponsePayload)
	if err != nil {
		return err
	}

	if pRespPayload.Extension == nil {
		return errors.New("response payload is missing extension")
	}

	ccAction, err := protoutil.UnmarshalChaincodeAction(pRespPayload.Extension)
	if err != nil {
		return err
	}

	txRWSet := &rwsetutil.TxRwSet{}

	if err = txRWSet.FromProtoBytes(ccAction.Results); err != nil {
		return err
	}


	for _, nsrws := range txRWSet.NsRwSets {
		for _, kvWrite := range nsrws.KvRwSet.Writes {
			fmt.Println("validateHashesOfTxRWS:", kvWrite.Key, kvWrite.Value)
			if hashesOfPreimages.exists(string(kvWrite.ValueHash)) {
				return errors.New("key wasn't found in pre-image space")

			} else {
				fmt.Println("key", kvWrite.Key, "write was found in pre-image space")
			}
		}
	}


	return nil
}

func (s set) exists(element string) bool {
	_, ok := s[element]
	return ok
}