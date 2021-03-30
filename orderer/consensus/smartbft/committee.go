/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft

import (
	"encoding/asn1"
)

type CommitteeMetadata struct {
	StateBlockNumber   uint64
	State              []byte
	Committers         []int32
	ExpectedCommitters int
	CommitteeID        int32
}

func (cm *CommitteeMetadata) committed(id int32) bool {
	for _, e := range cm.Committers {
		if e == id {
			return true
		}
	}
	return false
}

func (cm *CommitteeMetadata) shouldCommit(id int32) bool {
	if cm == nil {
		return false
	}
	if len(cm.Committers) >= cm.ExpectedCommitters {
		return false
	}
	return !cm.committed(id)
}

func (cm *CommitteeMetadata) Unmarshal(bytes []byte) error {
	if len(bytes) == 0 {
		return nil
	}
	_, err := asn1.Unmarshal(bytes, cm)
	return err
}

func (cm *CommitteeMetadata) Marshal() []byte {
	if cm == nil || (cm.CommitteeID == 0 && len(cm.Committers) == 0 && cm.StateBlockNumber == 0 && len(cm.State) == 0) {
		return nil
	}
	bytes, err := asn1.Marshal(*cm)
	if err != nil {
		panic(err)
	}
	return bytes
}
