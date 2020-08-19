/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	"bytes"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/common/crypto/tlsgen"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestX509CertExpiresAt(t *testing.T) {
	certBytes, err := ioutil.ReadFile(filepath.Join("testdata", "cert.pem"))
	assert.NoError(t, err)
	sId := &msp.SerializedIdentity{
		IdBytes: certBytes,
	}
	serializedIdentity, err := proto.Marshal(sId)
	assert.NoError(t, err)
	expirationTime := ExpiresAt(serializedIdentity)
	assert.Equal(t, time.Date(2027, 8, 17, 12, 19, 48, 0, time.UTC), expirationTime)
}

func TestX509InvalidCertExpiresAt(t *testing.T) {
	certBytes, err := ioutil.ReadFile(filepath.Join("testdata", "badCert.pem"))
	assert.NoError(t, err)
	sId := &msp.SerializedIdentity{
		IdBytes: certBytes,
	}
	serializedIdentity, err := proto.Marshal(sId)
	assert.NoError(t, err)
	expirationTime := ExpiresAt(serializedIdentity)
	assert.True(t, expirationTime.IsZero())
}

func TestIdemixIdentityExpiresAt(t *testing.T) {
	idemixId := &msp.SerializedIdemixIdentity{
		NymX: []byte{1, 2, 3},
		NymY: []byte{1, 2, 3},
		Ou:   []byte("OU1"),
	}
	idemixBytes, err := proto.Marshal(idemixId)
	assert.NoError(t, err)
	sId := &msp.SerializedIdentity{
		IdBytes: idemixBytes,
	}
	serializedIdentity, err := proto.Marshal(sId)
	assert.NoError(t, err)
	expirationTime := ExpiresAt(serializedIdentity)
	assert.True(t, expirationTime.IsZero())
}

func TestInvalidIdentityExpiresAt(t *testing.T) {
	expirationTime := ExpiresAt([]byte{1, 2, 3})
	assert.True(t, expirationTime.IsZero())
}

func TestTrackExpiration(t *testing.T) {
	ca, err := tlsgen.NewCA()
	assert.NoError(t, err)

	now := time.Now()
	expirationTime := certExpirationTime(ca.CertBytes())

	timeUntilExpiration := expirationTime.Sub(now)
	timeUntilOneMonthBeforeExpiration := timeUntilExpiration - 28*24*time.Hour
	timeUntil2DaysBeforeExpiration := timeUntilExpiration - 2*24*time.Hour - time.Hour*12

	monthBeforeExpiration := now.Add(timeUntilOneMonthBeforeExpiration)
	twoDaysBeforeExpiration := now.Add(timeUntil2DaysBeforeExpiration)

	tlsCert, err := ca.NewServerCertKeyPair("127.0.0.1")
	assert.NoError(t, err)

	signingIdentity := protoutil.MarshalOrPanic(&msp.SerializedIdentity{
		IdBytes: tlsCert.Cert,
	})

	shouldNotBeInvoked := func(format string, args ...interface{}) {
		t.Fatalf(format, args...)
	}

	var formattedWarning string
	shouldBeInvoked := func(format string, args ...interface{}) {
		formattedWarning = fmt.Sprintf(format, args...)
	}

	for _, testCase := range []struct {
		description     string
		tls             bool
		serverCert      []byte
		clientCertChain [][]byte
		sIDBytes        []byte
		warn            WarnFunc
		now             time.Time
		expectedWarn    string
	}{
		{
			description: "No TLS, enrollment cert isn't valid logs a warning",
			warn:        shouldNotBeInvoked,
			sIDBytes:    []byte{1, 2, 3},
		},
		{
			description:  "No TLS, enrollment cert expires soon",
			sIDBytes:     signingIdentity,
			warn:         shouldBeInvoked,
			now:          monthBeforeExpiration,
			expectedWarn: "The enrollment certificate will expire within one week",
		},
		{
			description:  "TLS, server cert expires soon",
			warn:         shouldBeInvoked,
			now:          monthBeforeExpiration,
			tls:          true,
			serverCert:   tlsCert.Cert,
			expectedWarn: "The server TLS certificate will expire within one week",
		},
		{
			description:  "TLS, server cert expires really soon",
			warn:         shouldBeInvoked,
			now:          twoDaysBeforeExpiration,
			tls:          true,
			serverCert:   tlsCert.Cert,
			expectedWarn: "The server TLS certificate expires within 2 days and 12 hours",
		},
		{
			description:  "TLS, server cert has expired",
			warn:         shouldBeInvoked,
			now:          expirationTime.Add(time.Hour),
			tls:          true,
			serverCert:   tlsCert.Cert,
			expectedWarn: "The server TLS certificate has expired",
		},
		{
			description:     "TLS, client cert expires soon",
			warn:            shouldBeInvoked,
			now:             monthBeforeExpiration,
			tls:             true,
			clientCertChain: [][]byte{tlsCert.Cert},
			expectedWarn:    "The client TLS certificate will expire within one week",
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			defer func() {
				formattedWarning = ""
			}()

			fakeTimeAfter := func(duration time.Duration, f func()) *time.Timer {
				assert.NotEmpty(t, testCase.expectedWarn)
				threeWeeks := 3 * 7 * 24 * time.Hour
				assert.Equal(t, threeWeeks, duration)
				f()
				return nil
			}

			TrackExpiration(testCase.tls,
				testCase.serverCert,
				testCase.clientCertChain,
				testCase.sIDBytes,
				testCase.warn,
				testCase.now,
				fakeTimeAfter)

			if testCase.expectedWarn != "" {
				assert.Equal(t, testCase.expectedWarn, formattedWarning)
			} else {
				assert.Empty(t, formattedWarning)
			}
		})
	}
}

func TestLogNonPubKeyMismatchErr(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	aliceKeyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	bobKeyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	expected := &bytes.Buffer{}
	expected.WriteString(fmt.Sprintf("Failed determining if public key of %s matches public key of %s: foo",
		string(aliceKeyPair.Cert),
		string(bobKeyPair.Cert)))

	b := &bytes.Buffer{}
	f := func(template string, args ...interface{}) {
		fmt.Fprintf(b, template, args...)
	}

	LogNonPubKeyMismatchErr(f, errors.New("foo"), aliceKeyPair.TLSCert.Raw, bobKeyPair.TLSCert.Raw)

	require.Equal(t, expected.String(), b.String())
}

func TestCertificatesWithSamePublicKey(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	bobKeyPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	bobCert := bobKeyPair.Cert
	bob := pem2der(bobCert)

	aliceCert := `-----BEGIN CERTIFICATE-----
MIIBNjCB3KADAgECAgELMAoGCCqGSM49BAMCMBAxDjAMBgNVBAUTBUFsaWNlMB4X
DTIwMDgxODIxMzU1NFoXDTIwMDgyMDIxMzU1NFowEDEOMAwGA1UEBRMFQWxpY2Uw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQjZP5VD/RaczoPFbA4gkt1qb54R6SP
J/V5oxkhDboG9xWi0wpyghaMGwwxC7Q9wegEnyOVp9nXoLrQ8LUJ5BfZoycwJTAO
BgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwIwCgYIKoZIzj0EAwID
SQAwRgIhAK4le5XgH5edyhaQ9Sz7sFz3Zc4bbhPAzt9zQUYnoqK+AiEA5zcyLB/4
Oqe93lroE6GF9W7UoCZFzD7lXsWku/dgFOU=
-----END CERTIFICATE-----`

	reIssuedAliceCert := `-----BEGIN CERTIFICATE-----
MIIBNDCB3KADAgECAgELMAoGCCqGSM49BAMCMBAxDjAMBgNVBAUTBUFsaWNlMB4X
DTIwMDgxODIxMzY1NFoXDTIwMDgyMDIxMzY1NFowEDEOMAwGA1UEBRMFQWxpY2Uw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQjZP5VD/RaczoPFbA4gkt1qb54R6SP
J/V5oxkhDboG9xWi0wpyghaMGwwxC7Q9wegEnyOVp9nXoLrQ8LUJ5BfZoycwJTAO
BgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwIwCgYIKoZIzj0EAwID
RwAwRAIgDc8WyXFvsxCk97KS7D/LdYJxMpDKdHNFqpzJT9LddlsCIEr8KcMd/t5p
cRv6rqxvy5M+t0DhRtiwCen70YCUsksb
-----END CERTIFICATE-----`

	alice := pem2der([]byte(aliceCert))
	aliceMakesComeback := pem2der([]byte(reIssuedAliceCert))

	for _, test := range []struct {
		description string
		errContains string
		first       []byte
		second      []byte
	}{
		{
			description: "Bad first certificate",
			errContains: "asn1:",
			first:       []byte{1, 2, 3},
			second:      bob,
		},

		{
			description: "Bad second certificate",
			errContains: "asn1:",
			first:       alice,
			second:      []byte{1, 2, 3},
		},

		{
			description: "Different certificate",
			errContains: ErrPubKeyMismatch.Error(),
			first:       alice,
			second:      bob,
		},

		{
			description: "Same certificate",
			first:       alice,
			second:      alice,
		},

		{
			description: "Same certificate but different validity period",
			first:       alice,
			second:      aliceMakesComeback,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			err := CertificatesWithSamePublicKey(test.first, test.second)
			if test.errContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errContains)
				return
			}

			require.NoError(t, err)
		})
	}
}

func pem2der(p []byte) []byte {
	b, _ := pem.Decode(p)
	return b.Bytes
}
