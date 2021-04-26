/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft_test

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hyperledger/fabric/protos/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	committee "github.com/SmartBFT-Go/randomcommittees/pkg"
	"github.com/hyperledger/fabric/common/crypto/tlsgen"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/genesis"
	"github.com/hyperledger/fabric/common/tools/configtxgen/configtxgentest"
	"github.com/hyperledger/fabric/common/tools/configtxgen/encoder"
	genesisconfig "github.com/hyperledger/fabric/common/tools/configtxgen/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus/smartbft"
	"github.com/hyperledger/fabric/orderer/consensus/smartbft/mocks"
	"github.com/hyperledger/fabric/protos/common"
	smartbft2 "github.com/hyperledger/fabric/protos/orderer/smartbft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func makeCommitteeConfigBlock(blockNum uint64, certMap map[uint64]string, committeeMD bool) *common.Block {
	profile := configtxgentest.Load(genesisconfig.SampleDevModeSmartBFTProfile)

	if !committeeMD {
		profile.Orderer.SmartBFT.Options.CommitteeConfig = nil
	}

	profile.Orderer.SmartBFT.Consenters = nil

	for id := 1; id <= 10; id++ {

		consenter := &smartbft2.Consenter{
			ConsenterId: uint64(id),
			SelectionPk: []byte(base64.StdEncoding.EncodeToString([]byte{1, 2, 3})),
		}

		if !committeeMD {
			consenter.SelectionPk = nil
		}

		profile.Orderer.SmartBFT.Consenters = append(profile.Orderer.SmartBFT.Consenters, consenter)

		consenter.ClientTlsCert = []byte(certMap[consenter.ConsenterId])
		consenter.ServerTlsCert = []byte(certMap[consenter.ConsenterId])
		consenter.Identity = []byte(certMap[consenter.ConsenterId])

	}

	channelGroup, err := encoder.NewChannelGroup(profile)
	if err != nil {
		panic(err)
	}

	configBlock := genesis.NewFactoryImpl(channelGroup).Block("test")
	if configBlock == nil {
		panic("nil config block")
	}

	configBlock.Header.Number = blockNum

	configBlock.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = utils.MarshalOrPanic(&common.Metadata{
		Value: utils.MarshalOrPanic(&common.LastConfig{Index: blockNum}),
	})

	return configBlock
}

func generateCerts(t *testing.T) (map[uint64]string, func()) {
	dir, err := ioutil.TempDir("/tmp", "testCommitteeRetrieverCurrentCommittee")
	assert.NoError(t, err)

	ca, err := tlsgen.NewCA()
	assert.NoError(t, err)

	m := make(map[uint64]string)
	for i := uint64(1); i <= 10; i++ {
		serverCert, err := ca.NewServerCertKeyPair("127.0.0.1")
		assert.NoError(t, err)

		serverCertPath := filepath.Join(dir, fmt.Sprintf("server-cert%d.pem", i))

		assert.NoError(t, ioutil.WriteFile(serverCertPath, serverCert.Cert, 0644))

		m[i] = serverCertPath
	}

	return m, func() {
		assert.NoError(t, os.RemoveAll(dir))
	}

}

func TestCommitteeRetrieverCurrentCommittee(t *testing.T) {
	defer flogging.Reset()
	flogging.ActivateSpec("test=DEBUG")

	allnodes := `[1: AQID 2: AQID 3: AQID 4: AQID 5: AQID 6: AQID 7: AQID 8: AQID 9: AQID 10: AQID]`
	_ = allnodes
	certMap, cleanup := generateCerts(t)
	defer cleanup()

	blockchain := []*common.Block{
		makeCommitteeConfigBlock(0, certMap, false),
		makeNonConfigBlock(1, 0),
		makeCommitteeConfigBlock(2, certMap, true),
		makeNonConfigBlock(3, 2),
		//makeNonConfigBlock(4, 2),
	}

	cs := &mocks.CommitteeSelection{}

	tests := []struct {
		blockHeight  uint64
		name         string
		want         string
		wantErr      bool
		skip         bool
		mustBeLogged string
	}{
		{
			name:        "genesis block without committee metadata",
			blockHeight: 1,
			want:        "[]",
			skip:        true,
		},
		{
			name:        "standard block without committee metadata",
			blockHeight: 2,
			want:        "[]",
			skip:        true,
		},
		{
			skip:        true,
			name:        "config block that activates committee selection",
			blockHeight: 3,
			want:        allnodes,
			mustBeLogged: "Committee metadata of last block (2) not defined yet, " +
				"this is either a new channel or an upgrade from an older version, returning all candidate nodes",
		},
		{
			skip:        true,
			name:        "regular block after committee selection has been activated",
			blockHeight: 4,
			want:        allnodes,
			mustBeLogged: "Committee metadata of last block (3) not defined yet, " +
				"this is either a new channel or an upgrade from an older version, returning all candidate nodes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				return
			}

			whatMustBeLoggedWasLogged := len(tt.mustBeLogged) == 0

			logger := flogging.MustGetLogger("test")
			logger = logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if strings.Contains(entry.Message, tt.mustBeLogged) {
					whatMustBeLoggedWasLogged = true
				}
				return nil
			}))
			ledger := &mocks.Ledger{}
			ledger.On("Block", mock.Anything).Return(func(seq uint64) *common.Block {
				return blockchain[int(seq)]
			})
			ledger.On("Height").Return(func() uint64 {
				return tt.blockHeight
			})

			cr := &smartbft.CommitteeRetriever{
				NewCommitteeSelection: func(_ committee.Logger) committee.Selection {
					return cs
				},
				Ledger: ledger,
				Logger: logger,
			}

			got, err := cr.CurrentCommittee()
			if (err != nil) != tt.wantErr {
				t.Errorf("CurrentCommittee() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got.String() != tt.want {
				t.Errorf("CurrentCommittee() got = %v, want %v", got.String(), tt.want)
			}

			if !whatMustBeLoggedWasLogged {
				t.Errorf("%s shouldn't been logged but hasn't", tt.mustBeLogged)
			}
		})
	}
}
