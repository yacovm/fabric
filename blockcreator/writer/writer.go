package writer

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/x509"
	"encoding/asn1"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	rand2 "math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric-protos-go/peer"
	utils2 "github.com/hyperledger/fabric/bccsp/utils"
	makeblocks "github.com/hyperledger/fabric/blockcreator/proto"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/util"
	msp2 "github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
)

type Config struct {
	PeerMSP           string
	OrdererMSP        string
	PeerKeyPath       string
	PeerCertPath      string
	OrdererKeyPath    string
	OrdererCertPath   string
	ChaincodeName     string
	ChaincodeVersion  string
	SupportsRedaction bool
}

type BlockMaker struct {
	Config
	*blockSigner
	peerSigningIdentity *signingIdentity
}

func NewBlockMaker(c Config) *BlockMaker {
	bm := &BlockMaker{
		Config:              c,
		peerSigningIdentity: newSigningIdentity(c.PeerKeyPath, c.PeerCertPath, c.PeerMSP),
		blockSigner:         newBlockSigner(c.OrdererKeyPath, c.OrdererCertPath, c.OrdererMSP),
	}
	return bm
}

func (bm *BlockMaker) GenerateBlocks(file string, startSeq, blockCount, transactionsPerBlock int) {
	blockSeqsToCreate := seq(startSeq, blockCount)
	close(blockSeqsToCreate)
	blockwriters := bm.createBlockWriters(file, blockSeqsToCreate, transactionsPerBlock)
	var bwWG sync.WaitGroup
	bwWG.Add(len(blockwriters))
	for _, bw := range blockwriters {
		go func(bw *blockWriter) {
			defer bwWG.Done()
			bw.createBlocks()
		}(bw)
	}
	bwWG.Wait()

	// Merge all seqs2BlockSizes into Index
	idx := &makeblocks.Index{
		FileToSequences: map[string]*makeblocks.Sequences{}}
	for _, bw := range blockwriters {
		idx.FileToSequences[bw.f.Name()] = &makeblocks.Sequences{}
		for _, seq := range bw.blockSequencesCreated {
			idx.FileToSequences[bw.f.Name()].Seq = append(idx.FileToSequences[bw.f.Name()].Seq, seq)
		}
	}

	// Write Index to file
	b, err := proto.Marshal(idx)
	if err != nil {
		panic(err)
	}

	if err := ioutil.WriteFile(fmt.Sprintf("%s-index", file), b, 0666); err != nil {
		panic(err)
	}
}

func seq(startSeq, count int) chan uint64 {
	blockSeqsToCreate := make(chan uint64, count)
	for i := 0; i < count; i++ {
		blockSeqsToCreate <- uint64(startSeq + i)
	}
	return blockSeqsToCreate
}

func (bm *BlockMaker) createBlockWriters(filePrefix string, blockSeqsToCreate chan uint64, transactionsPerBlock int) []*blockWriter {
	concurrency := runtime.NumCPU()
	blockWriters := make([]*blockWriter, concurrency)
	for i := 0; i < len(blockWriters); i++ {
		f, err := os.Create(fmt.Sprintf("%s%d", filePrefix, i))
		if err != nil {
			panic(err)
		}
		blockWriters[i] = &blockWriter{
			f:                    f,
			bm:                   bm,
			transactionsPerBlock: transactionsPerBlock,
			blockSeqsToCreate:    blockSeqsToCreate,
		}
	}
	return blockWriters
}

type blockWriter struct {
	f                     *os.File
	bm                    *BlockMaker
	transactionsPerBlock  int
	blockSeqsToCreate     chan uint64
	blockSequencesCreated []uint64
}

func (bw *blockWriter) createBlocks() {
	defer bw.f.Close()
	for seq := range bw.blockSeqsToCreate {
		bw.newBlock(bw.transactionsPerBlock, seq, bw.bm, bw.f)
		bw.blockSequencesCreated = append(bw.blockSequencesCreated, seq)
	}
}

func (bw *blockWriter) newBlock(transactionsPerBlock int, seq uint64, bm *BlockMaker, file *os.File) uint32 {
	txns := make([]*common.Envelope, transactionsPerBlock)
	for j := 0; j < len(txns); j++ {
		txns[j] = bm.makeTransaction()
	}
	block := bm.makeblock(seq, txns...)
	blockBytes, _ := proto.Marshal(block)
	blockLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(blockLenBytes, uint32(len(blockBytes)))
	if _, err := file.Write(blockLenBytes); err != nil {
		panic(err)
	}
	if _, err := file.Write(blockBytes); err != nil {
		panic(err)
	}
	return uint32(len(blockBytes))
}

func (bm *BlockMaker) makeTransaction() *common.Envelope {
	cis := bm.makeCIS()
	chHdr := protoutil.MakeChannelHeader(common.HeaderType_ENDORSER_TRANSACTION, 0, "testchannel", 0)
	nonce, _ := crypto.GetRandomNonce()
	sigHdr := protoutil.MakeSignatureHeader(bm.peerSigningIdentity.creator, nonce)
	chHdr.TxId = protoutil.ComputeTxID(nonce, bm.peerSigningIdentity.creator)
	ccHdrExt := &peer.ChaincodeHeaderExtension{ChaincodeId: cis.ChaincodeSpec.ChaincodeId}
	ccHdrExtBytes, _ := proto.Marshal(ccHdrExt)
	chHdr.Extension = ccHdrExtBytes
	hdr := protoutil.MakePayloadHeader(chHdr, sigHdr)
	hdrBytes, _ := proto.Marshal(hdr)

	ccPropPayloadBytes := makeChaincodeProposalPayloadBytes(cis)
	prop := &peer.Proposal{Header: hdrBytes, Payload: ccPropPayloadBytes}
	preImages, propResp := bm.createProposalResponse(hdrBytes)
	env, err := protoutil.CreateSignedTx(prop, bm.peerSigningIdentity, propResp)
	if err != nil {
		panic(err)
	}
	env.PreImages = preImages
	return env
}

func (bm *BlockMaker) makeblock(seq uint64, transactions ...*common.Envelope) *common.Block {
	// If applicable, move all pre-images to the block space
	var preImages [][]byte
	for _, tx := range transactions {
		if len(tx.PreImages) > 0 {
			preImages = append(preImages, tx.PreImages...)
			tx.PreImages = nil
		}
	}

	data := &common.BlockData{
		Data: asBlockData(transactions),
	}

	if len(preImages) > 0 {
		data.PreimageSpace = preImages
	}

	block := protoutil.NewBlock(seq, nil)
	block.Header.DataHash = protoutil.BlockDataHash(block.Data)
	block.Data = data

	ordererSigHdr, err := bm.blockSigner.NewSignatureHeader()
	if err != nil {
		panic(err)
	}
	blockSignature := &common.MetadataSignature{
		SignatureHeader: protoutil.MarshalOrPanic(ordererSigHdr),
	}

	// Note, this value is intentionally nil, as this metadata is only about the signature, there is no additional metadata
	// information required beyond the fact that the metadata item is signed.
	blockSignatureValue := []byte(nil)
	blockSignature.Signature = protoutil.SignOrPanic(bm, util.ConcatenateBytes(blockSignatureValue, blockSignature.SignatureHeader, protoutil.BlockHeaderBytes(block.Header)))
	block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: blockSignatureValue,
		Signatures: []*common.MetadataSignature{
			blockSignature,
		},
	})
	return block
}

func (bm *BlockMaker) makeCIS() *peer.ChaincodeInvocationSpec {
	input := &peer.ChaincodeInput{
		Args: [][]byte{[]byte("query"), []byte("a")},
	}
	spec := &peer.ChaincodeSpec{
		Type:        peer.ChaincodeSpec_Type(peer.ChaincodeSpec_Type_value["GOLANG"]),
		ChaincodeId: &peer.ChaincodeID{Path: "", Name: bm.ChaincodeName, Version: bm.ChaincodeVersion},
		Input:       input,
	}
	return &peer.ChaincodeInvocationSpec{ChaincodeSpec: spec}
}

func makeChaincodeProposalPayloadBytes(cis *peer.ChaincodeInvocationSpec) []byte {
	b, _ := proto.Marshal(cis)
	ccpp := &peer.ChaincodeProposalPayload{
		TransientMap: nil,
		Input:        b,
	}
	b, _ = proto.Marshal(ccpp)
	return b
}

func (bm *BlockMaker) createProposalResponse(hdrBytes []byte) ([][]byte, *peer.ProposalResponse) {
	preImages, resp, results, ccId := createProposalRespComponents(bm.SupportsRedaction, bm.ChaincodeName, bm.ChaincodeVersion, randomKVWrite(), randomKVWrite(), randomKVWrite(), randomKVWrite(), randomKVWrite())
	ccPropPayloadbytes := makeChaincodeProposalPayloadBytes(bm.makeCIS())
	propRes, err := protoutil.CreateProposalResponse(hdrBytes, ccPropPayloadbytes, resp, results, []byte(""), ccId, bm.peerSigningIdentity)
	if err != nil {
		panic(err)
	}
	return preImages, propRes
}

var (
	keyBuffs [][]byte
)

func init() {
	keyBuffs = make([][]byte, 10)
	for i := 0; i < len(keyBuffs); i++ {
		keyBuffs[i] = make([]byte, 16)
		rand.Read(keyBuffs[i])
	}
	rand2.Seed(time.Now().Unix())
}

func randomKVWrite() *kvrwset.KVWrite {
	keyIndex := rand2.Intn(len(keyBuffs))
	valueBuff := make([]byte, 32)
	rand.Read(valueBuff)
	return &kvrwset.KVWrite{
		Key:   hex.EncodeToString(keyBuffs[keyIndex]),
		Value: valueBuff,
	}
}

func createProposalRespComponents(supportsRedaction bool, namespace, version string, writes ...*kvrwset.KVWrite) (preImages [][]byte, resp *peer.Response, results []byte, ccId *peer.ChaincodeID) {
	kvs := &kvrwset.KVRWSet{
		Writes: writes,
	}

	if supportsRedaction {
		for _, write := range writes {
			write.ValueHash = util.ComputeSHA256(write.Value)
			preImages = append(preImages, write.Value)
			write.Value = nil
		}
	}
	kvsBytes, _ := proto.Marshal(kvs)
	rws := &rwset.TxReadWriteSet{
		DataModel: rwset.TxReadWriteSet_KV,
		NsRwset: []*rwset.NsReadWriteSet{
			{
				Namespace: namespace,
				Rwset:     kvsBytes,
			},
		},
	}
	results, _ = proto.Marshal(rws)
	ccId = &peer.ChaincodeID{
		Name:    namespace,
		Version: version,
	}
	resp = &peer.Response{
		Status:  200,
		Message: "OK",
	}
	return preImages, resp, results, ccId
}

func createCreator(clientCert string, mspID string) []byte {
	b, err := ioutil.ReadFile(clientCert)
	if err != nil {
		panic(err)
	}
	sId := &msp.SerializedIdentity{
		Mspid:   mspID,
		IdBytes: b,
	}
	b, err = proto.Marshal(sId)
	if err != nil {
		panic(err)
	}
	return b
}

type blockSigner struct {
	si *signingIdentity
}

func newBlockSigner(ordererKey, ordererCert, mspID string) *blockSigner {
	return &blockSigner{
		si: newSigningIdentity(ordererKey, ordererCert, mspID),
	}
}

func (bs *blockSigner) NewSignatureHeader() (*common.SignatureHeader, error) {
	nonce, _ := crypto.GetRandomNonce()
	return &common.SignatureHeader{
		Creator: bs.si.creator,
		Nonce:   nonce,
	}, nil
}

func (bs *blockSigner) Sign(message []byte) ([]byte, error) {
	return bs.si.Sign(message)
}

func newSigningIdentity(key, cert, mspID string) *signingIdentity {
	return &signingIdentity{
		creator:    createCreator(cert, mspID),
		PrivateKey: loadPrivateKey(key),
	}
}

type signingIdentity struct {
	*ecdsa.PrivateKey
	creator []byte
}

func (si *signingIdentity) GetIdentifier() *msp2.IdentityIdentifier {
	panic("implement me")
}

func (si *signingIdentity) GetMSPIdentifier() string {
	panic("implement me")
}

func (si *signingIdentity) Validate() error {
	panic("implement me")
}

func (si *signingIdentity) GetOrganizationalUnits() []*msp2.OUIdentifier {
	panic("implement me")
}

func (si *signingIdentity) Verify(msg []byte, sig []byte) error {
	panic("implement me")
}

func (si *signingIdentity) SatisfiesPrincipal(principal *msp.MSPPrincipal) error {
	panic("implement me")
}

func (si *signingIdentity) GetPublicVersion() msp2.Identity {
	panic("implement me")
}

func (si *signingIdentity) ExpiresAt() time.Time {
	return time.Time{}
}

func (si *signingIdentity) Serialize() ([]byte, error) {
	return si.creator, nil
}

func (si *signingIdentity) Sign(msg []byte) ([]byte, error) {
	digest := util.ComputeSHA256(msg)
	return signECDSA(si.PrivateKey, digest)
}

func loadPrivateKey(file string) *ecdsa.PrivateKey {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	bl, _ := pem.Decode(b)
	key, err := x509.ParsePKCS8PrivateKey(bl.Bytes)
	if err != nil {
		panic(err)
	}
	return key.(*ecdsa.PrivateKey)
}

func asBlockData(transactions []*common.Envelope) [][]byte {
	res := make([][]byte, len(transactions))
	for i, tx := range transactions {
		envBytes, _ := proto.Marshal(tx)
		res[i] = envBytes
	}
	return res
}

func signECDSA(k *ecdsa.PrivateKey, digest []byte) (signature []byte, err error) {
	r, s, err := ecdsa.Sign(rand.Reader, k, digest)
	if err != nil {
		return nil, err
	}

	utils2.ToLowS(&k.PublicKey, s)
	if err != nil {
		return nil, err
	}

	return marshalECDSASignature(r, s)
}

func marshalECDSASignature(r, s *big.Int) ([]byte, error) {
	return asn1.Marshal(ECDSASignature{r, s})
}

type ECDSASignature struct {
	R, S *big.Int
}
