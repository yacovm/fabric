/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft

import (
	"crypto/rand"
	"testing"

	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/stretchr/testify/require"
)

func TestCompressUncompressProposal(t *testing.T) {
	msg := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_PrePrepare{
			PrePrepare: &smartbftprotos.PrePrepare{
				Proposal: &smartbftprotos.Proposal{
					Payload:              make([]byte, 1024),
					Metadata:             []byte{1, 2, 3},
					Header:               []byte{4, 5, 6},
					VerificationSequence: 1,
				},
			},
		},
	}

	_, err := rand.Read(msg.GetPrePrepare().GetProposal().Payload)
	require.NoError(t, err)

	require.Equal(t, msg, decompressProposal(compressProposal(msg)))

}

func TestCompressUncompressProposalEmptyMessage(t *testing.T) {
	msg := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_PrePrepare{
			PrePrepare: &smartbftprotos.PrePrepare{
				Proposal: &smartbftprotos.Proposal{
					Payload:              make([]byte, 1024),
					Metadata:             []byte{1, 2, 3},
					Header:               []byte{4, 5, 6},
					VerificationSequence: 1,
				},
			},
		},
	}

	_, err := rand.Read(msg.GetPrePrepare().GetProposal().Payload)
	require.NoError(t, err)

	require.Equal(t, &smartbftprotos.Message{}, decompressProposal(compressProposal(&smartbftprotos.Message{})))
	require.Equal(t, &smartbftprotos.Message{
		Content: &smartbftprotos.Message_PrePrepare{},
	}, decompressProposal(compressProposal(&smartbftprotos.Message{
		Content: &smartbftprotos.Message_PrePrepare{},
	})))
	require.Equal(t, &smartbftprotos.Message{
		Content: &smartbftprotos.Message_PrePrepare{
			PrePrepare: &smartbftprotos.PrePrepare{},
		},
	}, decompressProposal(compressProposal(&smartbftprotos.Message{
		Content: &smartbftprotos.Message_PrePrepare{
			PrePrepare: &smartbftprotos.PrePrepare{},
		},
	})))

}
