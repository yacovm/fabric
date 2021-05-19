/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft

import (
	"bytes"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math"
	"net"
	"reflect"
	"sync/atomic"
	"time"

	types2 "github.com/hyperledger/fabric/orderer/consensus/smartbft/types"

	"github.com/hyperledger/fabric/common/util"

	smartbft "github.com/SmartBFT-Go/consensus/pkg/consensus"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/pkg/wal"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	cs "github.com/SmartBFT-Go/randomcommittees"
	committee "github.com/SmartBFT-Go/randomcommittees/pkg"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/etcdraft"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/msp"
	"github.com/hyperledger/fabric/protos/orderer"
	smartbft2 "github.com/hyperledger/fabric/protos/orderer/smartbft"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

//go:generate counterfeiter -o mocks/mock_blockpuller.go . BlockPuller

// BlockPuller is used to pull blocks from other OSN
type BlockPuller interface {
	PullBlock(seq uint64) *common.Block
	HeightsByEndpoints() (map[string]uint64, error)
	Close()
}

// WALConfig consensus specific configuration parameters from orderer.yaml; for SmartBFT only WALDir is relevant.
type WALConfig struct {
	WALDir            string // WAL data of <my-channel> is stored in WALDir/<my-channel>
	SnapDir           string // Snapshots of <my-channel> are stored in SnapDir/<my-channel>
	EvictionSuspicion string // Duration threshold that the node samples in order to suspect its eviction from the channel.
}

type ConfigValidator interface {
	ValidateConfig(env *common.Envelope) error
}

type signerSerializer interface {
	// Sign a message and return the signature over the digest, or error on failure
	Sign(message []byte) ([]byte, error)

	// Serialize converts an identity to bytes
	Serialize() ([]byte, error)
}

// BFTChain implements Chain interface to wire with
// BFT smart library
type BFTChain struct {
	committeeDisabled       bool
	egress                  *Egress
	committeePrivateKey     committee.PrivateKey
	ct                      *CommitteeTracker
	commitZKPWeSent         *atomic.Value
	commitZKPWeReceivedOdd  *atomic.Value
	commitZKPWeReceivedEven *atomic.Value
	cv                      ConfigValidator
	RuntimeConfig           *atomic.Value
	Channel                 string
	Config                  types.Configuration
	PullerConfig            pullerConfig
	Comm                    cluster.Communicator
	SignerSerializer        signerSerializer
	PolicyManager           policies.Manager
	Logger                  *flogging.FabricLogger
	WALDir                  string
	consensus               *smartbft.Consensus
	support                 consensus.ConsenterSupport
	verifier                *Verifier
	assembler               *Assembler
	Metrics                 *Metrics
	heartbeatMonitor        *AtomicHeartBeatMonitor
	streamPuller            *BlocksStreamPuller
}

// NewChain creates new BFT Smart chain
func NewChain(
	committeeDisabled bool,
	privateKeyBytesHash []byte,
	cv ConfigValidator,
	selfID uint64,
	config types.Configuration,
	walDir string,
	pc pullerConfig,
	comm cluster.Communicator,
	signerSerializer signerSerializer,
	policyManager policies.Manager,
	support consensus.ConsenterSupport,
	metrics *Metrics,
) (*BFTChain, error) {

	requestInspector := &RequestInspector{
		ValidateIdentityStructure: func(_ *msp.SerializedIdentity) error {
			return nil
		},
	}

	logger := flogging.MustGetLogger("orderer.consensus.smartbft.chain").With(zap.String("channel", support.ChainID()))

	committeeLogger := flogging.MustGetLogger("orderer.consensus.smartbft.committee").With(zap.String("channel", support.ChainID()))

	committeeSelection := cs.NewCommitteeSelection(committeeLogger)
	rndSeed := NewPRG(privateKeyBytesHash)
	publicKey, privateKey, err := committeeSelection.GenerateKeyPair(rndSeed)
	if err != nil {
		return nil, errors.Wrap(err, "failed generating committee selection key pair")
	}

	lastBlock := LastBlockFromLedgerOrPanic(support, logger)
	lastConfigBlock := LastConfigBlockFromLedgerOrPanic(support, logger)

	stdDialer := &cluster.StandardDialer{
		ClientConfig: pc.baseDialer.ClientConfig.Clone(),
	}
	stdDialer.ClientConfig.AsyncConnect = false
	stdDialer.ClientConfig.SecOpts.VerifyCertificate = nil

	der, _ := pem.Decode(stdDialer.ClientConfig.SecOpts.Certificate)
	if der == nil {
		return nil, errors.Errorf("client certificate isn't in PEM format: %v",
			string(stdDialer.ClientConfig.SecOpts.Certificate))
	}

	commitZKP := &atomic.Value{}
	commitZKP.Store([]byte{}) // Store an empty slice for type safety
	c := &BFTChain{
		heartbeatMonitor:        &AtomicHeartBeatMonitor{},
		committeeDisabled:       committeeDisabled,
		commitZKPWeReceivedOdd:  &atomic.Value{},
		commitZKPWeReceivedEven: &atomic.Value{},
		commitZKPWeSent:         commitZKP,
		committeePrivateKey:     privateKey,
		cv:                      cv,
		RuntimeConfig:           &atomic.Value{},
		Channel:                 support.ChainID(),
		Config:                  config,
		WALDir:                  walDir,
		Comm:                    comm,
		support:                 support,
		SignerSerializer:        signerSerializer,
		PolicyManager:           policyManager,
		PullerConfig:            pc,
		Logger:                  logger,
		Metrics: &Metrics{
			ClusterSize:          metrics.ClusterSize.With("channel", support.ChainID()),
			CommittedBlockNumber: metrics.CommittedBlockNumber.With("channel", support.ChainID()),
			IsLeader:             metrics.IsLeader.With("channel", support.ChainID()),
			LeaderID:             metrics.LeaderID.With("channel", support.ChainID()),
		},
		ct: &CommitteeTracker{
			committeeDisabled: committeeDisabled,
			logger:            committeeLogger,
			ledger:            &CachingLedger{Ledger: support},
		},
		streamPuller: &BlocksStreamPuller{
			Ledger:        support,
			Logger:        logger,
			Channel:       support.ChainID(),
			RetryTimeout:  500 * time.Millisecond,
			FetchTimeout:  time.Minute,
			LastBlock:     lastBlock,
			Signer:        support,
			BlockVerifier: support,
			TLSCert:       der.Bytes,
			Dialer:        stdDialer,
		},
	}

	c.streamPuller.OnBlockCommit = c.maybeAddedToCommittee
	c.streamPuller.CommitteeSize = func() int {
		return len(c.ct.CurrentCommittee())
	}

	rtc := RuntimeConfig{
		id:                selfID,
		logger:            logger,
		OnCommitteeChange: c.onCommitteeChange,
	}
	rtc, err = rtc.BlockCommitted(lastConfigBlock)
	if err != nil {
		return nil, errors.Wrap(err, "failed constructing RuntimeConfig")
	}
	rtc, err = rtc.BlockCommitted(lastBlock)
	if err != nil {
		return nil, errors.Wrap(err, "failed constructing RuntimeConfig")
	}

	c.RuntimeConfig.Store(rtc)

	currentCommittee := c.ct.CurrentCommittee()

	if !committeeDisabled && committeeHasPublicKeysDefined(currentCommittee) {
		err = committeeSelection.Initialize(int32(selfID), privateKey, currentCommittee)
		if err != nil {
			return nil, errors.Wrap(err, "failed initializing committee selection instance")
		}
		logger.Infof("Initialized committee selection for %d with public key %s", selfID, base64.StdEncoding.EncodeToString(publicKey))
		logger.Infof("Nodes: %v", currentCommittee)
	} else {
		logger.Infof("Committee selection is disabled")
	}

	c.verifier = buildVerifier(c, cv, c.RuntimeConfig, support, requestInspector, policyManager)
	c.consensus = bftSmartConsensusBuild(c.ct, c, requestInspector)

	c.consensus.Signer = &Signer{
		ID:               c.Config.SelfID,
		Logger:           flogging.MustGetLogger("orderer.consensus.smartbft.signer").With(zap.String("channel", c.support.ChainID())),
		SignerSerializer: c.SignerSerializer,
		LastConfigBlockNum: func(block *common.Block) uint64 {
			if isConfigBlock(block) {
				return block.Header.Number
			}

			return c.RuntimeConfig.Load().(RuntimeConfig).LastConfigBlock.Header.Number
		},
		HeartbeatMonitor:  c.heartbeatMonitor,
		CreateReconShares: c.createReconShares,
	}

	// Setup communication with list of remotes notes for the new channel
	c.Comm.Configure(c.support.ChainID(), rtc.RemoteNodes)

	if err := c.consensus.ValidateConfiguration(rtc.Nodes); err != nil {
		return nil, errors.Wrap(err, "failed to verify SmartBFT-Go configuration")
	}

	logger.Infof("SmartBFT-v3 is now servicing chain %s", support.ChainID())

	return c, nil
}

func bftSmartConsensusBuild(
	ct *CommitteeTracker,
	c *BFTChain,
	requestInspector *RequestInspector,
) *smartbft.Consensus {
	var err error

	rtc := c.RuntimeConfig.Load().(RuntimeConfig)

	latestMetadata, err := getViewMetadataFromBlock(rtc.LastBlock)
	if err != nil {
		c.Logger.Panicf("Failed extracting view metadata from ledger: %v", err)
	}

	var consensusWAL *wal.WriteAheadLogFile
	var walInitState [][]byte

	c.Logger.Infof("Initializing a WAL for chain %s, on dir: %s", c.support.ChainID(), c.WALDir)
	consensusWAL, walInitState, err = wal.InitializeAndReadAll(c.Logger, c.WALDir, wal.DefaultOptions())
	if err != nil {
		c.Logger.Panicf("failed to initialize a WAL for chain %s, err %s", c.support.ChainID(), err)
	}

	clusterSize := uint64(len(rtc.Nodes))

	// report cluster size
	c.Metrics.ClusterSize.Set(float64(clusterSize))

	sync := &Synchronizer{
		committeeDisabled: c.committeeDisabled,
		PullerConfig:      c.PullerConfig,
		selfID:            rtc.id,
		BlockToDecision:   c.blockToDecision,
		OnCommit:          c.updateRuntimeConfig,
		Support:           c.support,
		Logger:            c.Logger,
		LatestConfig: func() (types.Configuration, []uint64) {
			rtc := c.RuntimeConfig.Load().(RuntimeConfig)
			return rtc.BFTConfig, rtc.Nodes
		},
	}

	channelDecorator := zap.String("channel", c.support.ChainID())
	logger := flogging.MustGetLogger("orderer.consensus.smartbft.consensus").With(channelDecorator)

	c.assembler = &Assembler{
		CurrentCommittee: ct.CurrentCommittee,
		MaybeCommit:      c.maybeCommit,
		RuntimeConfig:    c.RuntimeConfig,
		VerificationSeq:  c.verifier.VerificationSequence,
		Logger:           flogging.MustGetLogger("orderer.consensus.smartbft.assembler").With(channelDecorator),
	}

	c.egress = &Egress{
		ConvertMessage: func(m *smartbftprotos.Message, channel string) *orderer.ConsensusRequest {
			msg := bftMsgToClusterMsg(m, channel)
			if prp := m.GetPrePrepare(); prp != nil {
				zkp := c.commitZKPWeSent.Load().([]byte)

				if zkp != nil && len(zkp) > 0 {
					msg.Metadata = zkp
				}
			}
			return msg
		},
		CommitteeTracker: c.ct,
		Channel:          c.support.ChainID(),
		Logger:           flogging.MustGetLogger("orderer.consensus.smartbft.egress").With(channelDecorator),
		RPC: &cluster.RPC{
			Logger:        flogging.MustGetLogger("orderer.consensus.smartbft.rpc").With(channelDecorator),
			Channel:       c.support.ChainID(),
			StreamsByType: cluster.NewStreamsByType(),
			Comm:          c.Comm,
			Timeout:       5 * time.Minute, // Externalize configuration
		},
	}

	consensus := &smartbft.Consensus{
		Config:   c.Config,
		Logger:   logger,
		Verifier: c.verifier,
		// Signer is initialized later (after heartbeat monitor)
		Metadata:          latestMetadata,
		WAL:               consensusWAL,
		WALInitialContent: walInitState, // Read from WAL entries
		Application:       c,
		Assembler:         c.assembler,
		RequestInspector:  requestInspector,
		Synchronizer:      sync,
		Comm:              c.egress,
		Scheduler:         time.NewTicker(time.Second).C,
		ViewChangerTicker: time.NewTicker(time.Second).C,
	}

	proposal, signatures := c.lastPersistedProposalAndSignatures()
	if proposal != nil {
		consensus.LastProposal = *proposal
		consensus.LastSignatures = signatures
	}

	c.reportIsLeader(proposal) // report the leader

	return consensus
}

func buildVerifier(
	c *BFTChain,
	cv ConfigValidator,
	runtimeConfig *atomic.Value,
	support consensus.ConsenterSupport,
	requestInspector *RequestInspector,
	policyManager policies.Manager,
) *Verifier {
	channelDecorator := zap.String("channel", support.ChainID())
	logger := flogging.MustGetLogger("orderer.consensus.smartbft.verifier").With(channelDecorator)
	return &Verifier{
		VerifyCommitment:      c.verifyCommitment,
		ConfigValidator:       cv,
		VerificationSequencer: support,
		ReqInspector:          requestInspector,
		Logger:                logger,
		RuntimeConfig:         runtimeConfig,
		ConsenterVerifier: &consenterVerifier{
			logger:        logger,
			channel:       support.ChainID(),
			policyManager: policyManager,
		},

		AccessController: &chainACL{
			policyManager: policyManager,
			Logger:        logger,
		},
		Ledger: support,
		VerifyReconShares: func(reconShares []committee.ReconShare) error {
			if c.committeeDisabled {
				return nil
			}
			for _, rcs := range reconShares {
				cs := c.committeeSelection(nonMember)
				if err := cs.VerifyReconShare(rcs); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func (c *BFTChain) isInCommittee() bool {
	for _, id := range c.ct.CurrentCommittee().IDs() {
		if uint64(id) == c.Config.SelfID {
			return true
		}
	}

	return false
}

func (c *BFTChain) setupHBM() {
	selfID := c.Config.SelfID
	currentCommittee := c.ct.CurrentCommittee()
	rtc := c.RuntimeConfig.Load().(RuntimeConfig)
	myRole, heartbeatSenders, heartbeatReceivers := setupHeartbeatMonitor(selfID, currentCommittee, rtc.Nodes)

	heartbeatTicker := time.NewTicker(1 * time.Second)
	heartbeatTimeout := 60 * time.Second
	heartbeatCount := uint64(5)

	hbm := NewHeartbeatMonitor(c.consensus.Comm.(MessageSender), heartbeatTicker.C, c.Logger, heartbeatTimeout, heartbeatCount, myRole, heartbeatSenders, heartbeatReceivers)
	hbm.ticker = heartbeatTicker
	c.heartbeatMonitor.Set(hbm)
	c.heartbeatMonitor.Start()
}

func (c *BFTChain) createReconShares() []committee.ReconShare {
	if c.committeeDisabled {
		c.Logger.Debugf("Committee selection is disabled")
		return nil
	}
	if !c.isInCommittee() {
		c.Logger.Debugf("Not in committee, will not send ReconShares")
		return nil
	}

	lastBlock := LastBlockFromLedgerOrPanic(c.ct.ledger, c.Logger)

	committeeSelection := c.committeeSelection(member)
	currState := c.committeeState(false)
	feedback, _, err := committeeSelection.Process(currState, committee.Input{})
	if err != nil {
		c.Logger.Panicf("Failed creating reconstruction shares: %v", err)
	}
	stateDigest := base64.StdEncoding.EncodeToString(util.ComputeSHA256(currState.ToBytes()))
	c.Logger.Debugf("Created %d ReconShares at last block %d for state with digest %s",
		len(feedback.ReconShares), lastBlock.Header.Number, stateDigest)
	return feedback.ReconShares
}

func (c *BFTChain) maybeAddedToCommittee(block *common.Block) {
	cm, err := types2.CommitteeMetadataFromBlock(block)
	if err != nil {
		c.Logger.Panicf("Failed extracting committee metadata from block: %v", err)
	}

	prevRTC := c.RuntimeConfig.Load().(RuntimeConfig)
	newRTC, err := prevRTC.BlockCommitted(block)
	if err != nil {
		c.Logger.Errorf("Failed constructing RuntimeConfig from block %d, halting chain", block.Header.Number)
		c.Halt()
	}
	c.RuntimeConfig.Store(newRTC)
	if utils.IsConfigBlock(block) {
		c.Comm.Configure(c.Channel, newRTC.RemoteNodes)
	}

	committeeChanged := cm != nil && cm.CommitteeShiftAt == int64(block.Header.Number)

	if !committeeChanged {
		return
	}

	defer c.setupHBM()

	if !c.isInCommittee() {
		committee := c.ct.CurrentCommittee().IDs()
		c.Logger.Debugf("Current committee: %v", committee)
		committeeIDs := make(map[uint64]struct{})
		for _, id := range committee {
			committeeIDs[uint64(id)] = struct{}{}
		}

		lastBlock := LastBlockFromLedgerOrPanic(c.support, c.Logger)
		c.streamPuller.Initialize(c.computeEndpoints(committeeIDs), lastBlock)
		return
	}

	c.Logger.Infof("We were added to the committee (%s)", c.ct.CurrentCommittee().IDs())
	c.streamPuller.Stop()
	c.updateConsensusInstance()
	c.consensus.WALInitialContent = nil

	c.Logger.Infof("Starting consensus")
	err = c.consensus.Start()
	if err != nil {
		c.Logger.Errorf("Failed starting consensus: %v", err)
	}
}

func (c *BFTChain) verifyCommitment(block *common.Block) error {
	// Ensure Metadata slice is of the right size
	if len(block.Metadata.Metadata) != len(common.BlockMetadataIndex_name) {
		return errors.Errorf("block metadata is of size %d but should be of size %d",
			len(block.Metadata.Metadata), len(common.BlockMetadataIndex_name))
	}

	signatureMetadata := &common.Metadata{}
	if err := proto.Unmarshal(block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES], signatureMetadata); err != nil {
		return errors.Wrap(err, "malformed signature metadata")
	}

	ordererMDFromBlock := &common.OrdererBlockMetadata{}
	if err := proto.Unmarshal(signatureMetadata.Value, ordererMDFromBlock); err != nil {
		return errors.Wrap(err, "malformed orderer metadata in block")
	}

	if atomic.LoadUint64(&c.assembler.SeqProposed) == block.Header.Number && c.Config.SelfID == c.consensus.GetLeaderID() {
		c.Logger.Debugf("We have proposed block %d ourselves, skipping verification", block.Header.Number)
		return nil
	}

	if len(ordererMDFromBlock.CommittteeCommitment) == 0 {
		c.Logger.Debugf("Block %d doesn't contain any commitment to verify", block.Header.Number)
		return nil
	}

	if c.committeeDisabled {
		return errors.Errorf("committee selection is disabled, but block contains commitment")
	}

	var cmt interface{}
	if block.Header.Number%2 == 0 {
		cmt = c.commitZKPWeReceivedEven.Load()
	} else {
		cmt = c.commitZKPWeReceivedOdd.Load()
	}

	commitment := cmt.(committee.Commitment)
	commitment.Data = ordererMDFromBlock.CommittteeCommitment

	c.Logger.Debugf("Verifying commitment from %d, for block %d with body (%s) and ZKP (%s)",
		commitment.From,
		block.Header.Number,
		base64.StdEncoding.EncodeToString(commitment.Data),
		base64.StdEncoding.EncodeToString(commitment.Proof))

	committeeSelection := c.committeeSelection(nonMember)

	if err := committeeSelection.VerifyCommitment(commitment); err != nil {
		c.Logger.Warnf("commitment {Data: %s, Proof %s:} for block %d is not valid",
			base64.StdEncoding.EncodeToString(commitment.Data),
			base64.StdEncoding.EncodeToString(commitment.Proof),
			block.Header.Number)
		return errors.Wrap(err, "commitment is invalid")
	}

	c.Logger.Debugf("Commitment from %d is valid", commitment.From)

	state := c.committeeState(true)
	_, newState, err := committeeSelection.Process(state, committee.Input{
		Commitments: []committee.Commitment{commitment},
	})
	if err != nil {
		c.Logger.Panicf("Failed processing committee update: %v", err)
	}

	currentCommittee := c.ct.CurrentCommittee()
	rtc := c.RuntimeConfig.Load().(RuntimeConfig)
	expectedCommitters := (len(currentCommittee)-1)/3 + 1
	committeeMD := CommitteeMetadataForProposal(c.Logger, commitment.Data, newState.ToBytes(), rtc.CommitteeMetadata,
		int64(block.Header.Number), expectedCommitters, rtc.committeeMinimumLifespan, currentCommittee, commitment.From)

	expected := committeeMD.Marshal()
	if !bytes.Equal(ordererMDFromBlock.CommitteeMetadata, expected) {
		gotCommitteeMD := &types2.CommitteeMetadata{}
		gotCommitteeMD.Unmarshal(ordererMDFromBlock.CommitteeMetadata)

		c.Logger.Warnf("Expected committee metadata \n%+v but got \n%+v", committeeMD, gotCommitteeMD)
		return errors.Errorf("received committee metadata is different than expected")
	}

	return nil
}

type committeeInstanceType bool

const (
	nonMember committeeInstanceType = false
	member    committeeInstanceType = true
)

func (c *BFTChain) committeeSelection(isMember committeeInstanceType) committee.Selection {
	currentCommittee := c.ct.CurrentCommittee()
	committeeSelection := cs.NewCommitteeSelection(c.Logger)

	selfID := int32(math.MaxInt32)
	var privateKey committee.PrivateKey

	if isMember {
		selfID = int32(c.Config.SelfID)
		privateKey = c.committeePrivateKey
	}

	err := committeeSelection.Initialize(selfID, privateKey, currentCommittee)
	if err != nil {
		c.Logger.Panicf("Failed initializing committee after commit: %v", err)
	}
	return committeeSelection
}

func (c *BFTChain) onCommitteeChange(prevCommittee []int32) {
	if c.committeeDisabled {
		c.Logger.Debugf("Committee selection is disabled")
		return
	}

	if c.consensus == nil {
		c.Logger.Warn("initializing the chain, consensus instance is not ready yet, not need for committee change at that time")
		return
	}

	currentCommittee := c.ct.CurrentCommittee()
	c.Logger.Infof("Changing committee from [%v], to [%v]", prevCommittee, currentCommittee.IDs())

	c.migrateTransactions(prevCommittee, currentCommittee.IDs())

	c.setupHBM()

	committeeIdentifiers := currentCommittee.IDs()
	c.Logger.Debugf("Current committee: %v", committeeIdentifiers)
	committeeIDs := make(map[uint64]struct{})
	for _, id := range committeeIdentifiers {
		committeeIDs[uint64(id)] = struct{}{}
	}

	selfID := int32(c.Config.SelfID)
	_, inCommittee := committeeIDs[uint64(selfID)]

	if inCommittee {
		c.Logger.Debugf("node is selected to be in current committee, committee members are %s", committeeIDs)
		c.updateConsensusInstance()
		return
	}
	c.Logger.Debugf("node wasn't selected to be in current committee, committee members are %v", committeeIDs)
}

func (c *BFTChain) launchPullerIfNeeded() {
	c.streamPuller.Stop()

	if c.isInCommittee() {
		return
	}

	committeeIDs := make(map[uint64]struct{})
	for _, id := range c.ct.CurrentCommittee().IDs() {
		committeeIDs[uint64(id)] = struct{}{}
	}

	lastBlock := LastBlockFromLedgerOrPanic(c.support, c.Logger)

	c.streamPuller.Initialize(c.computeEndpoints(committeeIDs), lastBlock)
	go func() {
		c.Logger.Debugf("Waiting for ")
		for c.consensus.GetLeaderID() != 0 {
			time.Sleep(time.Millisecond * 100)
		}
		c.streamPuller.ContinuouslyPullBlocks()
	}()
}

func (c *BFTChain) committeeNodes() []uint64 {
	var res []uint64
	for _, n := range c.ct.CurrentCommittee().IDs() {
		res = append(res, uint64(n))
	}
	return res
}

func (c *BFTChain) updateConsensusInstance() {
	lastBlock := LastBlockFromLedgerOrPanic(c.support, c.Logger)
	latestMetadata, err := getViewMetadataFromBlock(lastBlock)
	if err != nil {
		c.Logger.Panicf("Failed extracting view metadata from ledger: %v", err)
	}
	c.Logger.Debug("updating consensus metadata, last proposal and signatures")
	c.consensus.Metadata = latestMetadata
	proposal, signatures := c.lastPersistedProposalAndSignatures()
	if proposal != nil {
		c.consensus.LastProposal = *proposal
		c.consensus.LastSignatures = signatures
	}
}

func (c *BFTChain) computeEndpoints(committeeIDs map[uint64]struct{}) []cluster.EndpointCriteria {
	consensusMD := &smartbft2.ConfigMetadata{}
	if err := proto.Unmarshal(c.support.SharedConfig().ConsensusMetadata(), consensusMD); err != nil {
		c.Logger.Panicf(fmt.Sprintf("cannot unmarshal consensus metadata %s", err.Error()))
	}
	endpointsInCommittee := make(map[string]struct{})
	for _, consenter := range consensusMD.Consenters {
		if _, exists := committeeIDs[consenter.ConsenterId]; !exists {
			c.Logger.Debugf("%d %s is not in the committee", consenter.ConsenterId, consenter.Host)
			continue
		}
		endpointsInCommittee[consenter.Host] = struct{}{}
	}
	endpoints, err := etcdraft.EndpointconfigFromFromSupport(c.support)
	if err != nil {
		c.Logger.Panicf(fmt.Sprintf("cannot extract TLS CA certs and endpoint %s", err.Error()))
	}
	c.Logger.Debugf("Endpoints in committee: %v", endpointsInCommittee)
	if len(endpointsInCommittee) > 0 {
		var filteredEndpoints []cluster.EndpointCriteria
		var filteredEndpointsURIs []string
		for _, ep := range endpoints {
			host, _, err := net.SplitHostPort(ep.Endpoint)
			if err != nil {
				c.Logger.Warnf("Invalid host port string %s: %v", ep.Endpoint, err)
				continue
			}
			if _, exists := endpointsInCommittee[host]; !exists {
				continue
			}
			filteredEndpoints = append(filteredEndpoints, ep)
			filteredEndpointsURIs = append(filteredEndpointsURIs, ep.Endpoint)
		}
		endpoints = filteredEndpoints
		c.Logger.Debugf("Filtering out endpoints of nodes not in the committee, remaining endpoints: %v", filteredEndpointsURIs)
	} else {
		c.Logger.Debugf("Endpoints and consenter endpoints are disjoint, using the endpoints without filtering by committee")
	}

	return endpoints
}

func (c *BFTChain) migrateTransactions(prevCommittee, nextCommittee []int32) {
	prev := make(map[int32]struct{})
	for _, id := range prevCommittee {
		prev[id] = struct{}{}
	}

	var newNodes []int32
	for _, id := range nextCommittee {
		if _, exists := prev[id]; exists {
			continue
		}
		newNodes = append(newNodes, id)
	}

	if _, iWasInPrevCommittee := prev[int32(c.Config.SelfID)]; !iWasInPrevCommittee {
		c.Logger.Debugf("I was not in the previous committee, nothing to migrate")
		return
	}

	if len(newNodes) == 0 {
		c.Logger.Debugf("No new nodes have been added to the committee")
		return
	}

	if c.consensus == nil {
		c.Logger.Debugf("Consensus hasn't been initialized yet")
		return
	}

	requests, _ := c.consensus.Pool.NextRequests(math.MaxInt32, math.MaxUint64, false)

	c.Logger.Infof("Migrating transactions from %v to %v", prevCommittee, nextCommittee)

	c.Logger.Infof("Sending %d transactions to %v", len(requests), newNodes)
	for _, id := range newNodes {
		t1 := time.Now()
		go func(id int32) {
			defer func() {
				c.Logger.Debugf("Sending %d transactions to %v took %v", len(requests), id, time.Since(t1))
			}()
			for _, tx := range requests {
				c.egress.SendTransaction(uint64(id), tx)
			}
		}(id)
	}
}

func (c *BFTChain) HandleMessage(sender uint64, m *smartbftprotos.Message, metadata []byte) {
	c.Logger.Debugf("Message from %d", sender)

	if prp := m.GetPrePrepare(); prp != nil {
		if !c.verifySuspects(prp) {
			c.Logger.Warningf("Failed verifying suspects in pre-prepare")
			return
		}

		leader := c.consensus.GetLeaderID()
		if leader != sender {
			c.Logger.Warnf("Received pre-prepare from %d but it was not the leader, the leader is %d", sender, leader)
			return
		}

		c.Logger.Debugf("Received pre-prepare from %d with ZKP (%s)", sender, base64.StdEncoding.EncodeToString(metadata))

		if prp.Seq%2 == 0 {
			c.commitZKPWeReceivedEven.Store(committee.Commitment{
				From:  int32(sender),
				Proof: metadata,
			})
		} else {
			c.commitZKPWeReceivedOdd.Store(committee.Commitment{
				From:  int32(sender),
				Proof: metadata,
			})
		}
	}

	c.consensus.HandleMessage(sender, m)
}

func (c *BFTChain) getSuspectsFromSignatures(signatures []*smartbftprotos.Signature) []int32 {
	var allSuspects []int32
	for _, signature := range signatures {
		sig := &Signature{}
		if err := sig.Unmarshal(signature.Msg); err != nil {
			c.Logger.Warningf("Failed unmarshaling signature, error: %v", err)
			continue
		}
		aux := sig.CommitteeAuxiliaryInput
		committeeFeedback := &smartbft2.CommitteeFeedback{}
		if err := proto.Unmarshal(aux, committeeFeedback); err != nil {
			c.Logger.Warningf("Failed unmarshaling committeeFeedback, error: %v", err)
			continue
		}
		list := committeeFeedback.Suspects
		cleanList := removeDuplicates(list)
		allSuspects = append(allSuspects, cleanList...)
	}

	n := len(c.ct.CurrentCommittee())
	f := (n-1)/3 + 1

	return agreedSuspects(allSuspects, int32(f))
}

func (c *BFTChain) getSuspectsFromBlock(proposal *smartbftprotos.Proposal, seq uint64) (error, []int32, bool) {
	tuple := &ByteBufferTuple{}
	if err := tuple.FromBytes(proposal.Payload); err != nil {
		c.Logger.Warningf("Failed reading proposal payload, error: %v", err)
		return err, nil, false
	}
	metadata := &common.BlockMetadata{}
	if err := proto.Unmarshal(tuple.B, metadata); err != nil {
		c.Logger.Warningf("Failed unmarshaling block metadata, error: %v", err)
		return err, nil, false
	}
	if metadata == nil || len(metadata.Metadata) < len(common.BlockMetadataIndex_name) {
		c.Logger.Warningf("Block metadata is either missing or contains too few entries")
		return errors.Errorf("block metadata is either missing or too few entires"), nil, false
	}
	signatureMetadata := &common.Metadata{}
	if err := proto.Unmarshal(metadata.Metadata[common.BlockMetadataIndex_SIGNATURES], signatureMetadata); err != nil {
		c.Logger.Warningf("Failed unmarshaling block signature metadata, error: %v", err)
		return err, nil, false

	}
	ordererMDFromBlock := &common.OrdererBlockMetadata{}
	if err := proto.Unmarshal(signatureMetadata.Value, ordererMDFromBlock); err != nil {
		c.Logger.Warningf("Failed unmarshaling orderer block metadata, error: %v", err)
		return err, nil, false
	}

	committeeMD := &types2.CommitteeMetadata{}
	if err := committeeMD.Unmarshal(ordererMDFromBlock.CommitteeMetadata); err != nil {
		c.Logger.Warnf("Failed unmarshaling CommitteeMetadata: %v", err)
		return err, nil, false
	}

	if committeeMD.CommitteeShiftAt != int64(seq) && len(ordererMDFromBlock.HeartbeatSuspects) > 0 {
		c.Logger.Warnf("Committee metadata contains suspects but this block isn't a committee change block")
		return errors.Errorf("committee metadata contains suspects but this block isn't a committee change block"), nil, false
	}

	return nil, ordererMDFromBlock.HeartbeatSuspects, committeeMD.CommitteeShiftAt == int64(seq)
}

func (c *BFTChain) verifySuspects(prp *smartbftprotos.PrePrepare) bool {
	agreedSuspects := c.getSuspectsFromSignatures(prp.PrevCommitSignatures)
	err, blockSuspects, shouldContainSuspects := c.getSuspectsFromBlock(prp.Proposal, prp.Seq)
	if err != nil {
		return false
	}

	rtc := c.RuntimeConfig.Load().(RuntimeConfig)
	if rtc.isConfig {
		if len(blockSuspects) != 0 {
			c.Logger.Warningf("Last block was a config block, but the suggested suspects list is not empty")
			return false
		}
		c.Logger.Infof("Last block was a config block, therefore disregarding the suspects verification")
		return true
	}

	if !shouldContainSuspects {
		return true
	}

	c.Logger.Debugf("Previous block signers are suspicious of %v", agreedSuspects)
	c.Logger.Debugf("This block contains suspects: %v", blockSuspects)

	if len(agreedSuspects) != len(blockSuspects) {
		c.Logger.Warningf("Length of suspects list doesn't match, according to the signatures the length should be %d, while in the block the length is %d", len(agreedSuspects), len(blockSuspects))
		return false
	}

	for i, s := range agreedSuspects {
		if blockSuspects[i] != s {
			c.Logger.Warningf("Suspect doesn't match, according to the signatures the %d'th suspect should be %d, while in the block the suspect is %d", i, s, blockSuspects[i])
			return false
		}
	}
	return true
}

func setupHeartbeatMonitor(selfID uint64, currentCommittee []committee.Node, allNodes []uint64) (myRole Role, heartbeatSenders []uint64, heartbeatReceivers []uint64) {
	currentCommitteeIDs := make([]uint64, 0)
	for _, node := range currentCommittee {
		currentCommitteeIDs = append(currentCommitteeIDs, uint64(node.ID))
	}
	myRole = HeartbeatSender
	heartbeatReceivers = make([]uint64, 0)
	for _, node := range currentCommitteeIDs {
		if selfID == node {
			myRole = HeartbeatReceiver
		}
		heartbeatReceivers = append(heartbeatReceivers, node)
	}

	heartbeatSenders = make([]uint64, 0)
	for _, node := range allNodes {
		receiver := false
		for _, committeeNode := range currentCommitteeIDs {
			if node == committeeNode {
				receiver = true
				break
			}
		}
		if receiver {
			continue
		}
		heartbeatSenders = append(heartbeatSenders, node)
	}
	return myRole, heartbeatSenders, heartbeatReceivers
}

func (c *BFTChain) maybeCommit() ([]byte, []byte) {
	if c.committeeDisabled {
		c.Logger.Debugf("Committee selection disabled")
		return nil, nil
	}
	rtc := c.RuntimeConfig.Load().(RuntimeConfig)

	currentCommittee := c.ct.CurrentCommittee()

	if !committeeHasPublicKeysDefined(currentCommittee) {
		c.Logger.Debugf("Committee is lacking public keys, will not commit")
		return nil, nil
	}

	if !c.isInCommittee() {
		c.Logger.Debugf("I am not in the committee, skipping the commit")
	}

	expectedCommitters := (len(currentCommittee)-1)/3 + 1

	if !rtc.CommitteeMetadata.ShouldCommit(int32(rtc.id), expectedCommitters, c.Logger) {
		return nil, nil
	}

	state := c.committeeState(true)
	prevStateSize := len(state.ToBytes())

	committeeSelection := c.committeeSelection(member)
	feedback, _, err := committeeSelection.Process(c.committeeState(true), committee.Input{})
	if err != nil {
		c.Logger.Panicf("Failed processing library: %v", err)
	}
	if feedback.Commitment != nil {
		c.commitZKPWeSent.Store(feedback.Commitment.Proof)
		_, newState, err := committeeSelection.Process(state, committee.Input{
			Commitments: []committee.Commitment{*feedback.Commitment},
		})
		if err != nil {
			c.Logger.Panicf("Failed processing library: %v", err)
		}
		rawNewState := newState.ToBytes()
		c.Logger.Infof("Created commit (%s) with ZKP (%s) of %d bytes, state grows from %d to %d bytes",
			base64.StdEncoding.EncodeToString(feedback.Commitment.Data),
			base64.StdEncoding.EncodeToString(feedback.Commitment.Proof),
			len(feedback.Commitment.Data), prevStateSize, len(rawNewState))
		return feedback.Commitment.Data, rawNewState
	}
	c.Logger.Infof("Nothing to commit")
	c.commitZKPWeSent.Store([]byte{})
	return nil, nil
}

func (c *BFTChain) HandleRequest(sender uint64, req []byte) {
	if _, err := c.verifier.verifyRequest(req, false); err != nil {
		c.Logger.Warnf("Request from %d is invalid: %v", sender, err)
		return
	}
	c.Logger.Debugf("HandleRequest from %d", sender)
	c.consensus.SubmitRequest(req)
}

func (c *BFTChain) HandleHeartbeat(sender uint64) {
	c.Logger.Debugf("HandleHeartbeat from %d", sender)
	c.heartbeatMonitor.ProcessHeartbeat(sender)
}

func (c *BFTChain) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
	block, err := ProposalToBlock(proposal)
	if err != nil {
		c.Logger.Panicf("failed to read proposal, err: %s", err)
	}

	var sigs []*common.MetadataSignature
	var ordererBlockMetadata []byte

	var signers []uint64

	for _, s := range signatures {
		sig := &Signature{}
		if err := sig.Unmarshal(s.Msg); err != nil {
			c.Logger.Errorf("Failed unmarshaling signature from %d: %v", s.ID, err)
			c.Logger.Errorf("Offending signature Msg: %s", base64.StdEncoding.EncodeToString(s.Msg))
			c.Logger.Errorf("Offending signature Value: %s", base64.StdEncoding.EncodeToString(s.Value))
			c.Logger.Errorf("Halting chain.")
			c.Halt()
			return types.Reconfig{}
		}

		if ordererBlockMetadata == nil {
			ordererBlockMetadata = sig.OrdererBlockMetadata
		}

		cf := &smartbft2.CommitteeFeedback{}
		proto.Unmarshal(sig.CommitteeAuxiliaryInput, cf)
		c.Logger.Debugf("Signature from %d with CommitteeAuxiliaryInput containing %d ReconShares", s.ID, len(cf.Reconshares))

		sigs = append(sigs, &common.MetadataSignature{
			CommitteeAuxiliaryInput: sig.CommitteeAuxiliaryInput,
			AuxiliaryInput:          sig.AuxiliaryInput,
			Signature:               s.Value,
			// We do not put a signature header when we commit the block.
			// Instead, we put the nonce and the identifier and at validation
			// we reconstruct the signature header at runtime.
			// SignatureHeader: sig.SignatureHeader,
			Nonce:    sig.Nonce,
			SignerId: s.ID,
		})

		signers = append(signers, s.ID)
	}

	block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = utils.MarshalOrPanic(&common.Metadata{
		Value:      ordererBlockMetadata,
		Signatures: sigs,
	})

	var mdTotalSize int
	for _, md := range block.Metadata.Metadata {
		mdTotalSize += len(md)
	}

	c.Logger.Infof("Delivering proposal, writing block %d with %d transactions and metadata of total size %d with signatures from %v to the ledger, node id %d",
		block.Header.Number,
		len(block.Data.Data),
		mdTotalSize,
		signers,
		c.Config.SelfID)
	c.Metrics.CommittedBlockNumber.Set(float64(block.Header.Number)) // report the committed block number
	c.reportIsLeader(&proposal)                                      // report the leader
	if utils.IsConfigBlock(block) {

		c.support.WriteConfigBlock(block, nil)
	} else {
		c.support.WriteBlock(block, nil)
	}

	// TODO: call c.cs.Process() with the commitment from the block

	reconfig := c.updateRuntimeConfig(block)
	return reconfig
}

func (c *BFTChain) reportIsLeader(proposal *types.Proposal) {
	var viewNum uint64
	if proposal.Metadata == nil { // genesis block
		viewNum = 0
	} else {
		proposalMD := &smartbftprotos.ViewMetadata{}
		if err := proto.Unmarshal(proposal.Metadata, proposalMD); err != nil {
			c.Logger.Panicf("Failed unmarshaling smartbft metadata from proposal: %v", err)
		}
		viewNum = proposalMD.ViewId
	}

	nodes := c.RuntimeConfig.Load().(RuntimeConfig).Nodes
	n := uint64(len(nodes))
	leaderID := nodes[viewNum%n] // same calculation as done in the library

	c.Metrics.LeaderID.Set(float64(leaderID))

	if leaderID == c.Config.SelfID {
		c.Metrics.IsLeader.Set(1)
	} else {
		c.Metrics.IsLeader.Set(0)
	}

}

func (c *BFTChain) committeeState(forCommit bool) committee.State {
	cr := &CommitteeRetriever{
		NewCommitteeSelection: cs.NewCommitteeSelection,
		Logger:                c.Logger,
		Ledger:                c.ct.ledger,
	}

	return cr.CurrentState(forCommit)
}

func (c *BFTChain) updateRuntimeConfig(block *common.Block) types.Reconfig {
	prevRTC := c.RuntimeConfig.Load().(RuntimeConfig)
	newRTC, err := prevRTC.BlockCommitted(block)
	if err != nil {
		c.Logger.Errorf("Failed constructing RuntimeConfig from block %d, halting chain", block.Header.Number)
		c.Halt()
		return types.Reconfig{}
	}
	c.RuntimeConfig.Store(newRTC)
	if utils.IsConfigBlock(block) {
		c.Comm.Configure(c.Channel, newRTC.RemoteNodes)
	}

	cm, err := types2.CommitteeMetadataFromBlock(block)
	if err != nil {
		c.Logger.Panicf("Failed extracting committee metadata from block: %v", err)
	}

	committeeChanged := cm != nil && cm.CommitteeShiftAt == int64(block.Header.Number)

	c.launchPullerIfNeeded()

	currentNodes := c.committeeNodes()

	membershipDidNotChange := !committeeChanged

	if c.committeeDisabled {
		membershipDidNotChange = reflect.DeepEqual(newRTC.Nodes, prevRTC.Nodes)
	}

	configDidNotChange := reflect.DeepEqual(newRTC.BFTConfig, prevRTC.BFTConfig)
	noChangeDetected := membershipDidNotChange && configDidNotChange
	return types.Reconfig{
		InLatestDecision: !noChangeDetected,
		CurrentNodes:     currentNodes,
		CurrentConfig:    newRTC.BFTConfig,
	}
}

func (c *BFTChain) Order(env *common.Envelope, configSeq uint64) error {
	seq := c.support.Sequence()
	if configSeq < seq {
		c.Logger.Warnf("Normal message was validated against %d, although current config seq has advanced (%d)", configSeq, seq)
		if _, err := c.support.ProcessNormalMsg(env); err != nil {
			return errors.Errorf("bad normal message: %s", err)
		}
	}

	return c.submit(env, configSeq)
}

func (c *BFTChain) Configure(config *common.Envelope, configSeq uint64) error {
	var err error
	seq := c.support.Sequence()
	if configSeq < seq {
		c.Logger.Warnf("Normal message was validated against %d, although current config seq has advanced (%d)", configSeq, seq)
		if config, _, err = c.support.ProcessConfigMsg(config); err != nil {
			return errors.Errorf("bad normal message: %s", err)
		}
	}

	if err := c.cv.ValidateConfig(config); err != nil {
		return errors.Wrap(err, "illegal config update attempted")
	}

	return c.submit(config, configSeq)
}

func (c *BFTChain) submit(env *common.Envelope, configSeq uint64) error {
	reqBytes, err := proto.Marshal(env)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal request envelope")
	}

	c.Logger.Debugf("Consensus.SubmitRequest, node id %d", c.Config.SelfID)
	c.consensus.SubmitRequest(reqBytes)
	return nil
}

func (c *BFTChain) WaitReady() error {
	return nil
}

func (c *BFTChain) Errored() <-chan struct{} {
	// TODO: Implement Errored
	return nil
}

func (c *BFTChain) Start() {
	c.Logger.Debugf("Syncing at startup")
	c.consensus.Synchronizer.Sync()
	c.Logger.Debugf("Finished startup sync")

	c.setupHBM()

	if !c.isInCommittee() {
		c.launchPullerIfNeeded()
		return
	}
	c.consensus.Start()
}

func (c *BFTChain) Halt() {
	c.Logger.Infof("Shutting down chain")
	c.heartbeatMonitor.Close()
	c.consensus.Stop()
}

func (c *BFTChain) lastPersistedProposalAndSignatures() (*types.Proposal, []types.Signature) {
	lastBlock := LastBlockFromLedgerOrPanic(c.support, c.Logger)
	// initial report of the last committed block number
	c.Metrics.CommittedBlockNumber.Set(float64(lastBlock.Header.Number))
	decision := c.blockToDecision(lastBlock)
	return &decision.Proposal, decision.Signatures
}

func (c *BFTChain) blockToProposalWithoutSignaturesInMetadata(block *common.Block) types.Proposal {
	blockClone := proto.Clone(block).(*common.Block)
	if len(blockClone.Metadata.Metadata) > int(common.BlockMetadataIndex_SIGNATURES) {
		signatureMetadata := &common.Metadata{}
		// Nil out signatures because we carry them around separately in the library format.
		proto.Unmarshal(blockClone.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES], signatureMetadata)
		signatureMetadata.Signatures = nil
		blockClone.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = utils.MarshalOrPanic(signatureMetadata)
	}
	prop := types.Proposal{
		Header: blockClone.Header.Bytes(),
		Payload: (&ByteBufferTuple{
			A: utils.MarshalOrPanic(blockClone.Data),
			B: utils.MarshalOrPanic(blockClone.Metadata),
		}).ToBytes(),
		VerificationSequence: int64(c.verifier.VerificationSequence()),
	}

	if isConfigBlock(block) {
		prop.VerificationSequence--
	}

	return prop
}

func (c *BFTChain) blockToDecision(block *common.Block) *types.Decision {
	proposal := c.blockToProposalWithoutSignaturesInMetadata(block)
	if block.Header.Number == 0 {
		return &types.Decision{
			Proposal: proposal,
		}
	}

	signatureMetadata := &common.Metadata{}
	if err := proto.Unmarshal(block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES], signatureMetadata); err != nil {
		c.Logger.Panicf("Failed unmarshaling signatures from block metadata: %v", err)
	}

	ordererMDFromBlock := &common.OrdererBlockMetadata{}
	if err := proto.Unmarshal(signatureMetadata.Value, ordererMDFromBlock); err != nil {
		c.Logger.Panicf("Failed unmarshaling OrdererBlockMetadata from block signature metadata: %v", err)
	}

	proposal.Metadata = ordererMDFromBlock.ConsenterMetadata

	var signatures []types.Signature
	for _, sigMD := range signatureMetadata.Signatures {
		id := sigMD.SignerId
		sig := &Signature{
			Nonce:                   sigMD.Nonce,
			BlockHeader:             block.Header.Bytes(),
			OrdererBlockMetadata:    signatureMetadata.Value,
			AuxiliaryInput:          sigMD.AuxiliaryInput,
			CommitteeAuxiliaryInput: sigMD.CommitteeAuxiliaryInput,
			SignatureHeader:         sigMD.SignatureHeader,
		}
		prpf := &smartbftprotos.PreparesFrom{}
		if err := proto.Unmarshal(sigMD.AuxiliaryInput, prpf); err != nil {
			c.Logger.Errorf("Failed unmarshaling auxiliary data")
			continue
		}
		c.Logger.Infof("AuxiliaryInput[%d]: %v", id, prpf)
		signatures = append(signatures, types.Signature{
			Msg:   sig.Marshal(),
			Value: sigMD.Signature,
			ID:    id,
		})
	}

	return &types.Decision{
		Signatures: signatures,
		Proposal:   proposal,
	}
}

func (c *BFTChain) blockToID2Identities(block *common.Block) NodeIdentitiesByID {
	env := &common.Envelope{}
	if err := proto.Unmarshal(block.Data.Data[0], env); err != nil {
		c.Logger.Panicf("Failed unmarshaling envelope of previous config block: %v", err)
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(env)
	if err != nil {
		c.Logger.Panicf("Failed getting a new bundle from envelope of previous config block: %v", err)
	}
	oc, _ := bundle.OrdererConfig()
	if oc == nil {
		c.Logger.Panicf("Orderer config of previous config block is nil")
	}
	m := &smartbft2.ConfigMetadata{}
	if err := proto.Unmarshal(oc.ConsensusMetadata(), m); err != nil {
		c.Logger.Panicf("Failed to unmarshal consensus metadata: %v", err)
	}
	id2Identies := map[uint64][]byte{}
	for _, consenter := range m.Consenters {
		sanitizedID, err := crypto.SanitizeIdentity(consenter.Identity)
		if err != nil {
			c.Logger.Panicf("Failed to sanitize identity: %v", err)
		}
		id2Identies[consenter.ConsenterId] = sanitizedID
	}
	return id2Identies
}

type chainACL struct {
	policyManager policies.Manager
	Logger        *flogging.FabricLogger
}

func (c *chainACL) Evaluate(signatureSet []*common.SignedData) error {
	policy, ok := c.policyManager.GetPolicy(policies.ChannelWriters)
	if !ok {
		return fmt.Errorf("could not find policy %s", policies.ChannelWriters)
	}

	err := policy.Evaluate(signatureSet)
	if err != nil {
		c.Logger.Debugf("SigFilter evaluation failed: %s, policyName: %s", err.Error(), policies.ChannelWriters)
		return errors.Wrap(errors.WithStack(msgprocessor.ErrPermissionDenied), err.Error())
	}
	return nil

}
