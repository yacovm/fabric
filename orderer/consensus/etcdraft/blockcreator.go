/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/protoutil"
)

// blockCreator holds number and hash of latest block
// so that next block will be created based on it.
type blockCreator struct {
	hash   []byte
	number uint64

	logger *flogging.FabricLogger
}

// Yacov: This is whre the block is created by the Raft orderer leader
// before it passes consensus.
// Each transaction should contain its own pre-image space, however,
// the block's data hash computation is over the aggregated form of all bytes of all
// transactions.
// Since we need to preserve the hash integrity of the block,
// we need to carve out the pre-image space out of each transaction,
// and move all pre-images into a unified pre-image space inside the block.
// The block data (BlockData) struct contains a single field- Data, of type [][]byte.
// We can add an additional field which will be a pre-image space: PreImages [][]byte
// and it will be ignored during block data hash computation, but will still be carried along
// in the subsequent flow of the system (unless someone is copying it manually and then we'll need to chase down
// why it was stripped out...)
// GAL: so this is where most of the orderer's roll is done? who calls this?
// GAL: blk in new fmt
func (bc *blockCreator) createNextBlock(envs []*cb.Envelope) *cb.Block {
	data := &cb.BlockData{
		Data: make([][]byte, len(envs)),
	}

	var pis [][]byte

	var err error
	for i, env := range envs {
		pis = append(pis, env.PreImages...)
		env.PreImages = nil
		fmt.Println(len(env.PreImages), "pre-images in envelope")
		data.Data[i], err = proto.Marshal(env)
		if err != nil {
			bc.logger.Panicf("Could not marshal envelope: %s", err)
		}
	}

	bc.number++

	block := protoutil.NewBlock(bc.number, bc.hash)
	block.Header.DataHash = protoutil.BlockDataHash(data)
	block.Data = data
	block.Data.PreimageSpace = pis

	fmt.Println("Created block with", len(block.Data.PreimageSpace), "pre-images")

	bc.hash = protoutil.BlockHeaderHash(block.Header)
	return block
}
