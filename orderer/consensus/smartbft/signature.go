/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft

import (
	"encoding/asn1"

	"github.com/hyperledger/fabric/common/util"
)

type Signature struct {
	Nonce                   []byte
	SignatureHeader         []byte
	BlockHeader             []byte
	OrdererBlockMetadata    []byte
	AuxiliaryInput          []byte
	CommitteeAuxiliaryInput []byte
}

func (sig *Signature) Unmarshal(bytes []byte) error {
	_, err := asn1.Unmarshal(bytes, sig)
	return err
}

func (sig *Signature) Marshal() []byte {
	bytes, err := asn1.Marshal(*sig)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (sig Signature) AsBytes() []byte {
	msg2Sign := util.ConcatenateBytes(sig.OrdererBlockMetadata, sig.SignatureHeader, sig.BlockHeader, sig.AuxiliaryInput, sig.CommitteeAuxiliaryInput)
	return msg2Sign
}
