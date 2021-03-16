package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
)

func TestSelectionMain(t *testing.T) {
	gt := NewGomegaWithT(t)
	selection, err := Build("github.com/hyperledger/fabric/cmd/cs")
	gt.Expect(err).NotTo(HaveOccurred())
	defer CleanupBuildArtifacts()

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	pubKey := privKey.Public()
	require.NotNil(t, pubKey)
	keyBytes, err := x509.MarshalPKCS8PrivateKey(privKey)
	require.NoError(t, err)

	tempDir, err := ioutil.TempDir("/tmp", "selectionTest")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	keyPath := path.Join(tempDir, "signing.key")
	keyFile, err := os.Create(keyPath)
	require.NoError(t, err)
	_, err = keyFile.Write(keyBytes)
	require.NoError(t, err)
	keyFile.Close()

	cmd := exec.Command(selection, "detect", "--userKey", keyPath)
	process, err := Start(cmd, os.Stdout, nil)
	gt.Expect(err).NotTo(HaveOccurred())
	gt.Eventually(process).Should(Exit(0))
	gt.Expect(process.Out).To(gbytes.Say("Public key for committee selection"))
}
