/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft_test

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/orderer/consensus/smartbft"
	"github.com/hyperledger/fabric/protos/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/assert"
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
