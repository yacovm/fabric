/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft_test

import (
	"fmt"
	"testing"

	"crypto/sha256"
	"encoding/hex"

	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/consensus/smartbft"
	"github.com/hyperledger/fabric/orderer/consensus/smartbft/mocks"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	hashOfZero = hex.EncodeToString(sha256.New().Sum(nil))
)

func TestNodeIdentitiesByID(t *testing.T) {
	m := make(smartbft.NodeIdentitiesByID)
	for id := uint64(0); id < 4; id++ {
		m[id] = protoutil.MarshalOrPanic(&msp.SerializedIdentity{
			IdBytes: []byte(fmt.Sprintf("%d", id)),
			Mspid:   "OrdererOrg",
		})

		sID := &msp.SerializedIdentity{}
		err := proto.Unmarshal(m[id], sID)
		assert.NoError(t, err)

		id2, ok := m.IdentityToID(m[id])
		assert.True(t, ok)
		assert.Equal(t, id, id2)
	}

	_, ok := m.IdentityToID(protoutil.MarshalOrPanic(&msp.SerializedIdentity{
		IdBytes: []byte(fmt.Sprintf("%d", 4)),
		Mspid:   "OrdererOrg",
	}))

	assert.False(t, ok)

	_, ok = m.IdentityToID([]byte{1, 2, 3})
	assert.False(t, ok)
}

func TestVerifyProposal(t *testing.T) {
	logger := flogging.MustGetLogger("test")
	lastBlock := makeNonConfigBlock(19, 10)
	notLastBlock := makeNonConfigBlock(18, 10)
	lastConfigBlock := makeConfigBlock(10)

	ledger := &mocks.Ledger{}
	ledger.On("Height").Return(uint64(20))
	ledger.On("Block", uint64(19)).Return(lastBlock)
	ledger.On("Block", uint64(10)).Return(lastConfigBlock)

	sequencer := &mocks.Sequencer{}
	sequencer.On("Sequence").Return(uint64(12))

	ac := &mocks.AccessController{}
	ac.On("Evaluate", mock.Anything).Return(nil)

	bv := &mocks.BlockVerifier{}

	reqInspector := &smartbft.RequestInspector{
		ValidateIdentityStructure: func(_ *msp.SerializedIdentity) error {
			return nil
		},
	}

	lastHash := hex.EncodeToString(protoutil.BlockHeaderHash(lastBlock.Header))

	for _, testCase := range []struct {
		description                 string
		verificationSequence        uint64
		lastBlock                   *common.Block
		lastConfigBlockNum          uint64
		bftMetadataMutator          func([]byte) []byte
		ordererBlockMetadataMutator func(metadata *common.OrdererBlockMetadata)
		expectedErr                 string
	}{
		{
			description:                 "green path",
			verificationSequence:        12,
			lastBlock:                   lastBlock,
			lastConfigBlockNum:          lastConfigBlock.Header.Number,
			bftMetadataMutator:          noopMutator,
			ordererBlockMetadataMutator: noopOrdererBlockMetadataMutator,
		},
		{
			description:                 "wrong verification sequence",
			verificationSequence:        11,
			lastBlock:                   lastBlock,
			lastConfigBlockNum:          lastConfigBlock.Header.Number,
			bftMetadataMutator:          noopMutator,
			ordererBlockMetadataMutator: noopOrdererBlockMetadataMutator,
			expectedErr:                 "expected verification sequence 12, but proposal has 11",
		},
		{
			description:                 "wrong verification sequence",
			verificationSequence:        12,
			lastBlock:                   lastBlock,
			lastConfigBlockNum:          666,
			bftMetadataMutator:          noopMutator,
			ordererBlockMetadataMutator: noopOrdererBlockMetadataMutator,
			expectedErr:                 "last config in block orderer metadata points to 666 but our persisted last config is 10",
		},
		{
			description:                 "wrong verification sequence",
			verificationSequence:        12,
			lastBlock:                   notLastBlock,
			lastConfigBlockNum:          lastConfigBlock.Header.Number,
			bftMetadataMutator:          noopMutator,
			ordererBlockMetadataMutator: noopOrdererBlockMetadataMutator,
			expectedErr: fmt.Sprintf("previous header hash is %s but expected %s",
				hex.EncodeToString(protoutil.BlockHeaderHash(notLastBlock.Header)),
				hex.EncodeToString(protoutil.BlockHeaderHash(lastBlock.Header))),
		},
		{
			description:          "corrupt metadata",
			verificationSequence: 12,
			lastBlock:            lastBlock,
			lastConfigBlockNum:   lastConfigBlock.Header.Number,
			bftMetadataMutator: func([]byte) []byte {
				return []byte{1, 2, 3}
			},
			ordererBlockMetadataMutator: noopOrdererBlockMetadataMutator,
			expectedErr:                 "failed unmarshaling smartbft metadata from proposal: proto: smartbftprotos.ViewMetadata: illegal tag 0 (wire type 1)",
		},
		{
			description:          "corrupt metadata",
			verificationSequence: 12,
			lastBlock:            lastBlock,
			lastConfigBlockNum:   lastConfigBlock.Header.Number,
			bftMetadataMutator: func([]byte) []byte {
				return protoutil.MarshalOrPanic(&smartbftprotos.ViewMetadata{LatestSequence: 100, ViewId: 2})
			},
			ordererBlockMetadataMutator: noopOrdererBlockMetadataMutator,
			expectedErr:                 "expected metadata in block to be view_id:2 latest_sequence:100  but got view_id:2 latest_sequence:1 ",
		},
		{
			description:          "No last config",
			verificationSequence: 12,
			lastBlock:            lastBlock,
			lastConfigBlockNum:   lastConfigBlock.Header.Number,
			bftMetadataMutator:   noopMutator,
			ordererBlockMetadataMutator: func(metadata *common.OrdererBlockMetadata) {
				metadata.LastConfig = nil
			},
			expectedErr: "last config is nil",
		},
		{
			description:          "Mismatched last config",
			verificationSequence: 12,
			lastBlock:            lastBlock,
			lastConfigBlockNum:   lastConfigBlock.Header.Number,
			bftMetadataMutator:   noopMutator,
			ordererBlockMetadataMutator: func(metadata *common.OrdererBlockMetadata) {
				metadata.LastConfig.Index = 666
			},
			expectedErr: "last config in block orderer metadata points to 666 but our persisted last config is 10",
		},
		{
			description:          "Corrupt inner BFT metadata",
			verificationSequence: 12,
			lastBlock:            lastBlock,
			lastConfigBlockNum:   lastConfigBlock.Header.Number,
			bftMetadataMutator:   noopMutator,
			ordererBlockMetadataMutator: func(metadata *common.OrdererBlockMetadata) {
				metadata.ConsenterMetadata = []byte{1, 2, 3}
			},
			expectedErr: "failed unmarshaling smartbft metadata from block: proto: smartbftprotos.ViewMetadata: illegal tag 0 (wire type 1)",
		},
		{
			description:          "Mismatching inner BFT metadata",
			verificationSequence: 12,
			lastBlock:            lastBlock,
			lastConfigBlockNum:   lastConfigBlock.Header.Number,
			bftMetadataMutator:   noopMutator,
			ordererBlockMetadataMutator: func(metadata *common.OrdererBlockMetadata) {
				metadata.ConsenterMetadata = protoutil.MarshalOrPanic(&smartbftprotos.ViewMetadata{LatestSequence: 666})
			},
			expectedErr: "expected metadata in block to be view_id:2 latest_sequence:1  but got latest_sequence:666 ",
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			assembler := &smartbft.Assembler{
				VerificationSeq: func() uint64 {
					return testCase.verificationSequence
				},
				Logger:             logger,
				LastBlock:          testCase.lastBlock,
				LastConfigBlockNum: testCase.lastConfigBlockNum,
			}

			md := protoutil.MarshalOrPanic(&smartbftprotos.ViewMetadata{
				LatestSequence: 1,
				ViewId:         2,
			})

			proposal, _ := assembler.AssembleProposal(md, [][]byte{nonConfigTx})

			// Maybe mutate the BFT metadata
			proposal.Metadata = testCase.bftMetadataMutator(proposal.Metadata)

			// Unwrap the OrdererBlockMetadata
			tuple := &smartbft.ByteBufferTuple{}
			tuple.FromBytes(proposal.Payload)
			blockMD := &common.BlockMetadata{}
			assert.NoError(t, proto.Unmarshal(tuple.B, blockMD))

			sigMD := &common.Metadata{}
			assert.NoError(t, proto.Unmarshal(blockMD.Metadata[common.BlockMetadataIndex_SIGNATURES], sigMD))

			ordererMetadataFromSignature := &common.OrdererBlockMetadata{}
			assert.NoError(t, proto.Unmarshal(sigMD.Value, ordererMetadataFromSignature))

			// Mutate the OrdererBlockMetadata
			testCase.ordererBlockMetadataMutator(ordererMetadataFromSignature)

			// And fold it back into the block
			sigMD.Value = protoutil.MarshalOrPanic(ordererMetadataFromSignature)
			blockMD.Metadata[common.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(sigMD)
			tuple.B = protoutil.MarshalOrPanic(blockMD)
			proposal.Payload = tuple.ToBytes()

			v := &smartbft.Verifier{
				Logger:                 logger,
				Ledger:                 ledger,
				VerificationSequencer:  sequencer,
				AccessController:       ac,
				BlockVerifier:          bv,
				ReqInspector:           reqInspector,
				LastConfigBlockNum:     10,
				LastCommittedBlockHash: lastHash,
			}

			reqInfo, err := v.VerifyProposal(proposal)

			if testCase.expectedErr == "" {
				assert.NoError(t, err)
				assert.NotNil(t, reqInfo)
				assert.Len(t, reqInfo, 1)
				assert.Equal(t, hashOfZero, reqInfo[0].ClientID)
				assert.Equal(t, hashOfZero, reqInfo[0].ID)
				return
			}

			assert.EqualError(t, err, testCase.expectedErr)
			assert.Nil(t, reqInfo)
		})
	}
}

func noopMutator(b []byte) []byte {
	return b
}

func noopOrdererBlockMetadataMutator(_ *common.OrdererBlockMetadata) {
}
