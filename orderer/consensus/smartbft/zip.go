/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/SmartBFT-Go/consensus/smartbftprotos"
)

func compressProposal(prp *smartbftprotos.Message) *smartbftprotos.Message {
	// Invalid message is not compressed
	if prp == nil || prp.GetPrePrepare() == nil || prp.GetPrePrepare().GetProposal() == nil {
		return prp
	}

	prePrepare := prp.GetPrePrepare()
	proposal := prePrepare.GetProposal()

	payload := proposal.Payload
	buff := bytes.Buffer{}
	zipStream := gzip.NewWriter(&buff)
	_, err := zipStream.Write(payload)
	if err != nil {
		panic(err)
	}
	if err := zipStream.Flush(); err != nil {
		panic(err)
	}
	if err := zipStream.Close(); err != nil {
		panic(err)
	}

	res := &smartbftprotos.Message{}
	res.Content = &smartbftprotos.Message_PrePrepare{
		PrePrepare: &smartbftprotos.PrePrepare{
			Seq:                  prePrepare.Seq,
			View:                 prePrepare.View,
			PrevCommitSignatures: prePrepare.GetPrevCommitSignatures(),
			Proposal: &smartbftprotos.Proposal{
				Header:               proposal.Header,
				VerificationSequence: proposal.VerificationSequence,
				Metadata:             proposal.Metadata,
				Payload:              buff.Bytes(),
			},
		},
	}

	return res
}

func decompressProposal(prp *smartbftprotos.Message) *smartbftprotos.Message {
	// Invalid message is not de-compressed
	if prp == nil || prp.GetPrePrepare() == nil || prp.GetPrePrepare().GetProposal() == nil {
		return prp
	}

	prePrepare := prp.GetPrePrepare()
	proposal := prePrepare.GetProposal()

	// From this point onwards, proposal and prePrepare are not nil
	buff := bytes.NewReader(proposal.Payload)
	zipStream, err := gzip.NewReader(buff)
	if err != nil {
		panic(err)
	}
	uncompressedPayload := new(bytes.Buffer)
	if _, err := io.Copy(uncompressedPayload, zipStream); err != nil {
		panic(err)
	}

	res := &smartbftprotos.Message{}
	res.Content = &smartbftprotos.Message_PrePrepare{
		PrePrepare: &smartbftprotos.PrePrepare{
			Seq:                  prePrepare.Seq,
			View:                 prePrepare.View,
			PrevCommitSignatures: prePrepare.GetPrevCommitSignatures(),
			Proposal: &smartbftprotos.Proposal{
				Header:               proposal.Header,
				VerificationSequence: proposal.VerificationSequence,
				Metadata:             proposal.Metadata,
				Payload:              uncompressedPayload.Bytes(),
			},
		},
	}

	return res
}
