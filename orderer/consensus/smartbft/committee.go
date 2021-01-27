/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft

import (
	"encoding/asn1"
)

type CommitteeMetadata struct {
	ConfigHash []byte
	State      []byte
	ReconShare []byte
}

func (cm *CommitteeMetadata) Unmarshal(bytes []byte) error {
	if len(bytes) == 0 {
		return nil
	}
	_, err := asn1.Unmarshal(bytes, cm)
	return err
}

func (cm *CommitteeMetadata) Marshal() []byte {
	if len(cm.ConfigHash) == 0 && len(cm.State) == 0 && len(cm.ReconShare) == 0 {
		return nil
	}
	bytes, err := asn1.Marshal(*cm)
	if err != nil {
		panic(err)
	}
	return bytes
}
