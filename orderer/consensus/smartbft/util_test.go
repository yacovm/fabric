/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft

import (
	"io/ioutil"
	"testing"

	"encoding/base64"
	"path/filepath"

	"crypto/x509"
	"encoding/pem"

	"crypto/sha256"
	"encoding/hex"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/crypto/tlsgen"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/mocks/common/multichannel"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/msp"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/stretchr/testify/assert"
)

func TestNewBlockPuller(t *testing.T) {
	ca, err := tlsgen.NewCA()
	assert.NoError(t, err)

	blockBytes, err := ioutil.ReadFile("testdata/mychannel.block")
	assert.NoError(t, err)

	goodConfigBlock := &common.Block{}
	assert.NoError(t, proto.Unmarshal(blockBytes, goodConfigBlock))

	lastBlock := &common.Block{
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{{}, utils.MarshalOrPanic(&common.Metadata{
				Value: utils.MarshalOrPanic(&common.LastConfig{Index: 42}),
			})},
		},
	}

	cs := &multichannel.ConsenterSupport{
		HeightVal: 100,
		BlockByIndex: map[uint64]*common.Block{
			42: goodConfigBlock,
			99: lastBlock,
		},
	}

	dialer := &cluster.PredicateDialer{
		ClientConfig: comm.ClientConfig{
			SecOpts: &comm.SecureOptions{
				Certificate: ca.CertBytes(),
			},
		},
	}

	blockPuller, err := newBlockPuller(cs, dialer,
		localconfig.Cluster{
			ReplicationMaxRetries: 2,
		})
	assert.NoError(t, err)
	assert.NotNil(t, blockPuller)
	blockPuller.Close()
}

func TestIsConsenterOfChannel(t *testing.T) {
	certInsideConfigBlock, err := base64.StdEncoding.DecodeString("LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUNmekNDQWlhZ0F3SUJBZ0lSQUlMS" +
		"ThQL1ZwTXZIUWxJTTJkRWZ0aHd3Q2dZSUtvWkl6ajBFQXdJd2JERUwKTUFrR0ExVUVCaE1DVlZNeEV6QVJCZ05WQkFnVENrTmhiR2xtYjNKdWFXRXhGakFVQmdOVkJBY1R" +
		"EVk5oYmlCRwpjbUZ1WTJselkyOHhGREFTQmdOVkJBb1RDMlY0WVcxd2JHVXVZMjl0TVJvd0dBWURWUVFERXhGMGJITmpZUzVsCmVHRnRjR3hsTG1OdmJUQWVGdzB4T1RFe" +
		"U1UWXhNVEU0TURCYUZ3MHlPVEV5TVRNeE1URTRNREJhTUZreEN6QUoKQmdOVkJBWVRBbFZUTVJNd0VRWURWUVFJRXdwRFlXeHBabTl5Ym1saE1SWXdGQVlEVlFRSEV3MVR" +
		"ZVzRnUm5KaApibU5wYzJOdk1SMHdHd1lEVlFRREV4UnZjbVJsY21WeU5DNWxlR0Z0Y0d4bExtTnZiVEJaTUJNR0J5cUdTTTQ5CkFnRUdDQ3FHU000OUF3RUhBMElBQkF4Z" +
		"UJxaVRnN21TK1dURGpvd0c4V3pmeFI2L1FHNEFxdHJYaTJRNTBKMUwKY2xkdzF4bTdJejQ4THZqa3pTTjNRMm9peFRHVjB6ZFFzcUtuOWNyRXp0MmpnYnN3Z2Jnd0RnWUR" +
		"WUjBQQVFILwpCQVFEQWdXZ01CMEdBMVVkSlFRV01CUUdDQ3NHQVFVRkJ3TUJCZ2dyQmdFRkJRY0RBakFNQmdOVkhSTUJBZjhFCkFqQUFNQ3NHQTFVZEl3UWtNQ0tBSUUzY" +
		"VEwV2IrSXlSM0xFa2RmNWRyaUoyYlU0MXYvcUNTQkxMQlhXazZxSHMKTUV3R0ExVWRFUVJGTUVPQ0ZHOXlaR1Z5WlhJMExtVjRZVzF3YkdVdVkyOXRnZ2h2Y21SbGNtVnl" +
		"OSUlKYkc5agpZV3hvYjNOMGh3Ui9BQUFCaHhBQUFBQUFBQUFBQUFBQUFBQUFBQUFCTUFvR0NDcUdTTTQ5QkFNQ0EwY0FNRVFDCklFTGdpYzNSSU9QRjFucll2Rit2STBTc" +
		"mVPN1dEc2FTM0NOaDZqdUdkTGlOQWlCOHdSYVRXY3ZaKzg4Qkxwc3QKVkdnUE9PbVAzRTJPeVJVMEpTWFFMamlHeFE9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==")
	assert.NoError(t, err)

	loadBlock := func(fileName string) *common.Block {
		b, err := ioutil.ReadFile(filepath.Join("testdata", fileName))
		assert.NoError(t, err)
		block := &common.Block{}
		err = proto.Unmarshal(b, block)
		assert.NoError(t, err)
		return block
	}
	for _, testCase := range []struct {
		name          string
		expectedError string
		configBlock   *common.Block
		certificate   []byte
	}{
		{
			name:          "nil block",
			expectedError: "nil block",
		},
		{
			name:          "no block data",
			expectedError: "block data is nil",
			configBlock:   &common.Block{},
		},
		{
			name: "invalid envelope inside block",
			expectedError: "failed to unmarshal payload from envelope:" +
				" error unmarshaling Payload: proto: common.Payload: illegal tag 0 (wire type 1)",
			configBlock: &common.Block{
				Data: &common.BlockData{
					Data: [][]byte{utils.MarshalOrPanic(&common.Envelope{
						Payload: []byte{1, 2, 3},
					})},
				},
			},
		},
		{
			name:          "valid config block with cert mismatch",
			configBlock:   loadBlock("smartbft_genesis_block.pb"),
			certificate:   certInsideConfigBlock[2:],
			expectedError: cluster.ErrNotInChannel.Error(),
		},
		{
			name:          "etcdraft genesis block",
			configBlock:   loadBlock("etcdraftgenesis.block"),
			expectedError: "not a SmartBFT config block",
		},
		{
			name:        "valid config block with matching cert",
			configBlock: loadBlock("smartbft_genesis_block.pb"),
			certificate: certInsideConfigBlock,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := ConsenterCertificate(testCase.certificate).IsConsenterOfChannel(testCase.configBlock)
			if testCase.expectedError != "" {
				assert.EqualError(t, err, testCase.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

const (
	highSCACert = `LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJwRENDQVV1Z0F3SUJBZ0lSQUozQm9QcytuS2J0VnBVVFAzZG5pYWt3Q2dZSUtvWkl6ajBFQXdJd01qRXcKTUM0R0ExVUVCUk1uTWpBNU5qazBNVGN6TnpJek1URXhPRFF3TkRVek5UVTNNelUwTURVNU9EUTVOVEF4TURrMwpNQjRYRFRFNU1UQXlNVEV5TlRrd05Wb1hEVEk1TVRBeE9URXlOVGt3TlZvd01qRXdNQzRHQTFVRUJSTW5NakE1Ck5qazBNVGN6TnpJek1URXhPRFF3TkRVek5UVTNNelUwTURVNU9EUTVOVEF4TURrM01Ga3dFd1lIS29aSXpqMEMKQVFZSUtvWkl6ajBEQVFjRFFnQUVZU1QxTjhHT3h2VGJnQi93eGlZbGJ5UU1rTExCNWtTTmlmSDBXaWJDK3BBbgpvMHFIOUdNWEwxK1B5RGFLUlpNUGRMQ3NCa1o4Z0NHSEJXWjZZM28xaWFOQ01FQXdEZ1lEVlIwUEFRSC9CQVFECkFnR21NQjBHQTFVZEpRUVdNQlFHQ0NzR0FRVUZCd01DQmdnckJnRUZCUWNEQVRBUEJnTlZIUk1CQWY4RUJUQUQKQVFIL01Bb0dDQ3FHU000OUJBTUNBMGNBTUVRQ0lGWkhpZGNLeG9NcDB4RTNuM0lydW5rczlLQUZlaHhlaUt6Rgo4NURHMnRGOEFpQWJkdTFwc2pWK1c0WWpGZ3pyK2N3MUxVYUlFeTVmcGZ4ZTNjU1BtUm9sL0E9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==`
	highSCert   = `LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJwVENDQVVxZ0F3SUJBZ0lRV29oOFNWZnlhRFZwcDN5TkFROHdWVEFLQmdncWhrak9QUVFEQWpBeU1UQXcKTGdZRFZRUUZFeWN5TURrMk9UUXhOek0zTWpNeE1URTROREEwTlRNMU5UY3pOVFF3TlRrNE5EazFNREV3T1RjdwpIaGNOTVRreE1ESXhNVEkxT1RBMVdoY05Namt4TURFNU1USTFPVEExV2pBeU1UQXdMZ1lEVlFRRkV5Y3hNakF6Ck16a3hPVEk0TWpNd05qZ3hNamswT0RFME5qQTJNREk1TkRNd05Ua3lNVEF6TWpVd1dUQVRCZ2NxaGtqT1BRSUIKQmdncWhrak9QUU1CQndOQ0FBVGs4ci9zZ1BKL2FwL2dZakw2T0dwcWc5TmRtd3dFSlp1OXFaaDAvYXRvbFNsVQp5V3cxUDdRR283Zk5rcVdXSi8xZm5jbUZ4ZTQzOTJEVmNJZERSTENYbzBJd1FEQU9CZ05WSFE4QkFmOEVCQU1DCkJhQXdIUVlEVlIwbEJCWXdGQVlJS3dZQkJRVUhBd0lHQ0NzR0FRVUZCd01CTUE4R0ExVWRFUVFJTUFhSEJIOEEKQUFFd0NnWUlLb1pJemowRUF3SURTUUF3UmdJaEFMdXZBSjlpUWJBVEFHMFRFanlqRmhuY3kwOVczQUpJbm91eQpvVnFZL3owNUFpRUE3QVhETkNLY3c3TU92dm0zTFFrMEJsdkRPSXNkRm5hMG96Rkp4RU0vdWRzPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==`
	lowSCert    = `LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJwVENDQVV1Z0F3SUJBZ0lSQVBGeXYrdzVkNjEybm95M0V5VXBYdHN3Q2dZSUtvWkl6ajBFQXdJd01qRXcKTUM0R0ExVUVCUk1uTWpreU5qTXlNakUzTkRZeU1qQXdOVGswTWpjMU5qSXhOekU0TXpVM01UYzVPVGt6TmpFeQpNQjRYRFRFNU1UQXlNVEV6TURBek5Gb1hEVEk1TVRBeE9URXpNREF6TkZvd01qRXdNQzRHQTFVRUJSTW5Nekl3Ck9UTTVOell4TkRneE9UQXpOamN6TkRRME56azNORGM0Tmprek5UQXhORGt5T1RVMU1Ga3dFd1lIS29aSXpqMEMKQVFZSUtvWkl6ajBEQVFjRFFnQUVhZ0NmSDlIS1ZHMEs3S1BUclBUQVpGMGlHZFNES3E2b3E2cG9KVUI5dFI0ZgpXRDN5cEJQZ0xrSDd6R25yL0wrVERIQnVIZGEwNHROYkVha1BwVzhCdnFOQ01FQXdEZ1lEVlIwUEFRSC9CQVFECkFnV2dNQjBHQTFVZEpRUVdNQlFHQ0NzR0FRVUZCd01DQmdnckJnRUZCUWNEQVRBUEJnTlZIUkVFQ0RBR2h3Ui8KQUFBQk1Bb0dDQ3FHU000OUJBTUNBMGdBTUVVQ0lRQ2xCb2ZiNEZRREs1TDJxdjRWMTdaWHdHVm9LQWxuK1lmMQpReVNGblZIVk1BSWdNNzd4ZVBnQ3BNQ3BsOVFyb2ROQi9TV2tCWlZ4VGdKVlpmeWJBMFR3bGcwPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==`
)

func TestSanitizeIdentity(t *testing.T) {
	extractCertFromPEM := func(cert []byte) *x509.Certificate {
		bl, _ := pem.Decode(cert)

		certificate, err := x509.ParseCertificate(bl.Bytes)
		assert.NoError(t, err)

		return certificate
	}

	logger := flogging.MustGetLogger("test")
	t.Run("lowS stays the same", func(t *testing.T) {
		cert, err := base64.StdEncoding.DecodeString(lowSCert)
		assert.NoError(t, err)

		identity := &msp.SerializedIdentity{
			Mspid:   "SampleOrg",
			IdBytes: cert,
		}
		identityPreSanitation := utils.MarshalOrPanic(identity)
		identityAfterSanitation := SanitizeIdentity(identityPreSanitation, logger)
		assert.Equal(t, identityPreSanitation, identityAfterSanitation)
	})

	t.Run("highS changes, but is still verifiable under the CA", func(t *testing.T) {
		cert, err := base64.StdEncoding.DecodeString(highSCert)
		assert.NoError(t, err)

		caCert, err := base64.StdEncoding.DecodeString(highSCACert)
		assert.NoError(t, err)

		identity := &msp.SerializedIdentity{
			Mspid:   "SampleOrg",
			IdBytes: cert,
		}
		identityPreSanitation := utils.MarshalOrPanic(identity)
		identityAfterSanitation := SanitizeIdentity(identityPreSanitation, logger)
		assert.NotEqual(t, identityPreSanitation, identityAfterSanitation)

		err = proto.Unmarshal(identityAfterSanitation, identity)
		assert.NoError(t, err)
		certAfterSanitation := identity.IdBytes

		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(caCert)

		_, err = extractCertFromPEM(certAfterSanitation).Verify(x509.VerifyOptions{
			Roots: certPool,
		})
	})
}

func makeTx(nonce, creator []byte) []byte {
	return utils.MarshalOrPanic(&common.Envelope{
		Payload: utils.MarshalOrPanic(&common.Payload{
			Header: &common.Header{
				ChannelHeader: utils.MarshalOrPanic(&common.ChannelHeader{
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
					ChannelId: "test-chain",
				}),
				SignatureHeader: utils.MarshalOrPanic(&common.SignatureHeader{
					Creator: creator,
					Nonce:   nonce,
				}),
			},
		}),
	})
}

func TestRequestID(t *testing.T) {
	ri := &RequestInspector{
		ValidateIdentityStructure: func(identity *msp.SerializedIdentity) error {
			return nil
		},
	}

	nonce := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	creator := utils.MarshalOrPanic(&msp.SerializedIdentity{
		Mspid:   "SampleOrg",
		IdBytes: []byte{1, 2, 3},
	})

	tx := makeTx(nonce, creator)

	var expectedTxID []byte
	expectedTxID = append(expectedTxID, nonce...)
	expectedTxID = append(expectedTxID, creator...)

	txID := sha256.Sum256(expectedTxID)
	expectedTxString := hex.EncodeToString(txID[:])

	expectedClient := sha256.Sum256(creator)

	info := ri.RequestID(tx)
	assert.Equal(t, expectedTxString, info.ID)
	assert.Equal(t, hex.EncodeToString(expectedClient[:]), info.ClientID)
}
