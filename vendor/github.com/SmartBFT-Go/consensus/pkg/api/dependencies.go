// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package api

import (
	bft "github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

type Application interface {
	Deliver(proposal bft.Proposal, signature []bft.Signature)
}

type Comm interface {
	SendConsensus(targetID uint64, m *protos.Message)
	SendTransaction(targetID uint64, request []byte)
	Nodes() []uint64
}

type Assembler interface {
	AssembleProposal(metadata []byte, requests [][]byte) (nextProp bft.Proposal, remainder [][]byte)
}

type WriteAheadLog interface {
	Append(entry []byte, truncateTo bool) error
}

type Signer interface {
	Sign([]byte) []byte
	SignProposal(bft.Proposal) *bft.Signature
}

type Verifier interface {
	VerifyProposal(proposal bft.Proposal) ([]bft.RequestInfo, error)
	VerifyRequest(val []byte) (bft.RequestInfo, error)
	VerifyConsenterSig(signature bft.Signature, prop bft.Proposal) error
	VerifySignature(signature bft.Signature) error
	VerificationSequence() uint64
}

type RequestInspector interface {
	RequestID(req []byte) bft.RequestInfo
}

type Synchronizer interface {
	Sync() bft.Decision
}

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}
