/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliverclient

import (
	"fmt"
	"math"
	"net"
	"sync/atomic"
	"time"

	cs "github.com/SmartBFT-Go/randomcommittees"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/gossip/api"
	gossipcommon "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/consensus/smartbft"
	"github.com/hyperledger/fabric/orderer/consensus/smartbft/types"
	"github.com/hyperledger/fabric/protos/common"
	gossip_proto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/hyperledger/fabric/protos/orderer"
	smartbft_protos "github.com/hyperledger/fabric/protos/orderer/smartbft"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

//go:generate mockery -dir . -name LedgerInfo -case underscore -output ../mocks/

// LedgerInfo an adapter to provide the interface to query
// the ledger committer for current ledger height
type LedgerInfo interface {
	// LedgerHeight returns current local ledger height
	LedgerHeight() (uint64, error)
}

//go:generate mockery -dir . -name Ledger -case underscore -output mocks

type Ledger interface {
	LedgerInfo

	// Gets blocks with sequence numbers provided in the slice.
	GetBlocks(blockSeqs []uint64) []*common.Block
}

// SimpleLedger implements a simple ledger interface.
type SimpleLedger struct {
	ledger Ledger
}

// Height returns the number of blocks in the ledger this channel is associated with.
func (sl *SimpleLedger) Height() uint64 {
	h, err := sl.ledger.LedgerHeight()
	if err != nil {
		panic(fmt.Sprintf("Can't get ledger height, err: %v", err))
	}
	return h
}

// Block returns a block with the given number,
// or nil if such a block doesn't exist.
func (sl *SimpleLedger) Block(number uint64) *common.Block {
	blocks := sl.ledger.GetBlocks([]uint64{number})
	return blocks[0]
}

func lastConfigBlockFromLedger(ledger SimpleLedger) (*common.Block, error) {
	lastBlockSeq := ledger.Height() - 1
	lastBlock := ledger.Block(lastBlockSeq)
	if lastBlock == nil {
		return nil, errors.Errorf("unable to retrieve block [%d]", lastBlockSeq)
	}
	lastConfigBlock, err := cluster.LastConfigBlock(lastBlock, &ledger)
	if err != nil {
		return nil, err
	}
	return lastConfigBlock, nil
}

type GossipChannelConfig struct {
	AC channelconfig.Application
	OC channelconfig.Orderer
	configtx.Validator
	channelconfig.Channel
}

// ApplicationOrgs defines a mapping from ApplicationOrg by ID.
type ApplicationOrgs map[string]channelconfig.ApplicationOrg

func (gcp *GossipChannelConfig) ApplicationOrgs() ApplicationOrgs {
	return gcp.AC.Organizations()
}

func (gcp *GossipChannelConfig) OrdererOrgs() []string {
	var res []string
	for _, org := range gcp.OC.Organizations() {
		res = append(res, org.MSPID())
	}
	return res
}

func (gcp *GossipChannelConfig) OrdererAddressesByOrgs() map[string][]string {
	res := make(map[string][]string)
	for _, ordererOrg := range gcp.OC.Organizations() {
		if len(ordererOrg.Endpoints()) == 0 {
			continue
		}
		res[ordererOrg.MSPID()] = ordererOrg.Endpoints()
	}
	return res
}

// GossipServiceAdapter serves to provide basic functionality
// required from gossip service by delivery service
type GossipServiceAdapter interface {
	// PeersOfChannel returns slice with members of specified channel
	PeersOfChannel(gossipcommon.ChainID) []discovery.NetworkMember

	// AddPayload adds payload to the local state sync buffer
	AddPayload(chainID string, payload *gossip_proto.Payload) error

	// Gossip the message across the peers
	Gossip(msg *gossip_proto.GossipMessage)
}

// BlocksProvider used to read blocks from the ordering service
// for specified chain it subscribed to
type BlocksProvider interface {
	// DeliverBlocks starts delivering and disseminating blocks
	DeliverBlocks()

	// Stop shutdowns blocks provider and stops delivering new blocks
	Stop()
}

// BlocksDeliverer defines interface which actually helps
// to abstract the AtomicBroadcast_DeliverClient with only
// required method for blocks provider.
// This also decouples the production implementation of the gRPC stream
// from the code in order for the code to be more modular and testable.
type BlocksDeliverer interface {
	// Recv retrieves a response from the ordering service
	Recv() (*orderer.DeliverResponse, error)

	// Send sends an envelope to the ordering service
	Send(*common.Envelope) error
}

//go:generate mockery -dir . -name StreamClient -case underscore -output ../mocks/

type StreamClient interface {
	BlocksDeliverer

	// Close closes the stream and its underlying connection
	Close()

	// Disconnect disconnects from the remote node.
	Disconnect()

	// Update the client on the last valid block number
	UpdateReceived(blockNumber uint64)

	// UpdateEndpoints updates the endpoint of the underlying client
	UpdateEndpoints(endpoints []comm.EndpointCriteria)

	// GetEndpoint retrieves the orderer endpoint from which the client is receiving blocks (as opposed to headers)
	GetEndpoint() string
}

// blocksProviderImpl the actual implementation for BlocksProvider interface
type blocksProviderImpl struct {
	chainID string

	client StreamClient

	gossip GossipServiceAdapter

	mcs api.MessageCryptoService

	ledger Ledger

	done int32

	wrongStatusThreshold int
}

const wrongStatusThreshold = 10

var maxRetryDelay = time.Second * 10

// NewBlocksProvider constructor function to create blocks deliverer instance
func NewBlocksProvider(chainID string, client StreamClient, gossip GossipServiceAdapter, mcs api.MessageCryptoService, ledger Ledger) BlocksProvider {
	return &blocksProviderImpl{
		chainID:              chainID,
		client:               client,
		gossip:               gossip,
		mcs:                  mcs,
		ledger:               ledger,
		wrongStatusThreshold: wrongStatusThreshold,
	}
}

// DeliverBlocks used to pull out blocks from the ordering service to
// distributed them across peers
func (b *blocksProviderImpl) DeliverBlocks() {
	errorStatusCounter := 0
	var statusCounter uint64 = 0
	var verErrCounter uint64 = 0
	var delay time.Duration

	defer b.client.Close()
	for !b.isDone() {
		msg, err := b.client.Recv()
		if err != nil {
			logger.Warningf("[%s] Receive error: %s", b.chainID, err.Error())
			return
		}
		switch t := msg.Type.(type) {
		case *orderer.DeliverResponse_Status:
			verErrCounter = 0

			if t.Status == common.Status_SUCCESS {
				logger.Warningf("[%s] ERROR! Received success for a seek that should never complete", b.chainID)
				return
			}
			if t.Status == common.Status_BAD_REQUEST || t.Status == common.Status_FORBIDDEN {
				logger.Errorf("[%s] Got error %v", b.chainID, t)
				errorStatusCounter++
				if errorStatusCounter > b.wrongStatusThreshold {
					logger.Criticalf("[%s] Wrong statuses threshold passed, stopping block provider", b.chainID)
					return
				}
			} else {
				errorStatusCounter = 0
				logger.Warningf("[%s] Got error %v", b.chainID, t)
			}

			delay, statusCounter = computeBackOffDelay(statusCounter)
			time.Sleep(delay)
			b.client.Disconnect()
			continue
		case *orderer.DeliverResponse_Block:
			errorStatusCounter = 0
			statusCounter = 0
			blockNum := t.Block.Header.Number

			marshaledBlock, err := proto.Marshal(t.Block)
			if err != nil {
				logger.Errorf("[%s] Error serializing block with sequence number %d, due to %s; Disconnecting client from orderer.", b.chainID, blockNum, err)
				delay, verErrCounter = computeBackOffDelay(verErrCounter)
				time.Sleep(delay)
				b.client.Disconnect()
				continue
			}

			if err := b.mcs.VerifyBlock(gossipcommon.ChainID(b.chainID), blockNum, marshaledBlock); err != nil {
				logger.Errorf("[%s] Error verifying block with sequence number %d, due to %s; Disconnecting client from orderer.", b.chainID, blockNum, err)
				delay, verErrCounter = computeBackOffDelay(verErrCounter)
				time.Sleep(delay)
				b.client.Disconnect()
				continue
			}

			verErrCounter = 0 // On a good block

			numberOfPeers := len(b.gossip.PeersOfChannel(gossipcommon.ChainID(b.chainID)))
			// Create payload with a block received
			payload := createPayload(blockNum, marshaledBlock)
			// Use payload to create gossip message
			gossipMsg := createGossipMsg(b.chainID, payload)

			logger.Debugf("[%s] Adding payload to local buffer, blockNum = [%d]", b.chainID, blockNum)
			// Add payload to local state payloads buffer
			if err := b.gossip.AddPayload(b.chainID, payload); err != nil {
				logger.Warningf("Block [%d] received from ordering service wasn't added to payload buffer: %v", blockNum, err)
			}

			// Gossip messages with other nodes
			logger.Debugf("[%s] Gossiping block [%d], peers number [%d]", b.chainID, blockNum, numberOfPeers)
			if !b.isDone() {
				b.gossip.Gossip(gossipMsg)
			}

			b.client.UpdateReceived(blockNum)

			committeeDisabled := viper.GetBool("peer.deliveryclient.bft.committeeDisabled")

			if !committeeDisabled && !b.isCommitteeChangeBlock(t.Block) {
				logger.Infof("[%s] Not updating endpoints since this is not a committee change block , blockNum = [%d]", b.chainID, blockNum)
				continue
			}

			// wait for the block to be committed
			currHeight := blockNum
			for currHeight < blockNum+1 {
				time.Sleep(time.Millisecond)
				currHeight, err = b.ledger.LedgerHeight()
				if err != nil {
					logger.Panicf("[%s] Error getting ledger height while handling block with sequence number %d, due to %s", b.chainID, blockNum, err)
				}
			}

			b.client.UpdateEndpoints(b.getEndpoints(blockNum, committeeDisabled))

		default:
			logger.Warningf("[%s] Received unknown: %v", b.chainID, t)
			return
		}
	}
}

// Stop stops blocks delivery provider
func (b *blocksProviderImpl) Stop() {
	atomic.StoreInt32(&b.done, 1)
	b.client.Close()
}

// Check whenever provider is stopped
func (b *blocksProviderImpl) isDone() bool {
	return atomic.LoadInt32(&b.done) == 1
}

// Check if in this block there is a committee change
func (b *blocksProviderImpl) isCommitteeChangeBlock(block *common.Block) bool {
	md := utils.GetOrdererblockMetadataOrPanic(block).CommitteeMetadata
	if len(md) == 0 {
		return false
	}
	cm := &types.CommitteeMetadata{}
	if err := cm.Unmarshal(md); err != nil {
		logger.Panicf("[%s] Error unmarshaling committee metadata of block with sequence number %d, due to %s", b.chainID, block.Header.Number, err)
	}
	return cm.CommitteeShiftAt == int64(block.Header.Number)
}

func (b *blocksProviderImpl) getEndpoints(blockNum uint64, committeeDisabled bool) []comm.EndpointCriteria {
	simpleLedger := SimpleLedger{ledger: b.ledger}
	lastConfigBlock, err := lastConfigBlockFromLedger(simpleLedger)
	if err != nil {
		logger.Panicf("[%s] Error getting last config block while handling block with sequence number %d, due to %s", b.chainID, blockNum, err)
	}

	envelopeConfig, err := utils.ExtractEnvelope(lastConfigBlock, 0)
	if err != nil {
		logger.Panicf("[%s] Error getting envelope from config block while handling block with sequence number %d, due to %s", b.chainID, blockNum, err)
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(envelopeConfig)
	if err != nil {
		logger.Panicf("[%s] Error getting new bundle while handling block with sequence number %d, due to %s", b.chainID, blockNum, err)
	}
	oc, exists := bundle.OrdererConfig()
	if !exists {
		logger.Panicf("[%s] No orderer config in bundle while handling block with sequence number %d, due to %s", b.chainID, blockNum, err)
	}
	ac, exists := bundle.ApplicationConfig()
	if !exists {
		logger.Panicf("[%s] No application config in bundle while handling block with sequence number %d, due to %s", b.chainID, blockNum, err)
	}

	gcc := &GossipChannelConfig{
		Validator: bundle.ConfigtxValidator(),
		AC:        ac,
		OC:        oc,
		Channel:   bundle.ChannelConfig(),
	}

	cc := &ConnectionCriteria{
		OrdererEndpointsByOrg: gcc.OrdererAddressesByOrgs(),
		Organizations:         gcc.OrdererOrgs(),
		OrdererEndpoints:      gcc.OrdererAddresses(),
	}

	endpoints := cc.toEndpointCriteria()

	if committeeDisabled {
		logger.Infof("[%s] Committee is disabled, returning all endpoints, block [%d]", b.chainID, blockNum)
		return endpoints
	}

	logger.Infof("[%s] Committee is enabled, filtering endpoints by committee, block [%d]", b.chainID, blockNum)

	cr := &smartbft.CommitteeRetriever{
		//committeeDisabled:     false,
		NewCommitteeSelection: cs.NewCommitteeSelection,
		Logger:                flogging.MustGetLogger("orderer.consensus.smartbft.committee"),
		Ledger:                &simpleLedger,
	}

	currentCommittee, err := cr.CurrentCommittee()
	if err != nil {
		logger.Panicf("[%s] Error getting current committee while handling block with sequence number %d, due to %s", b.chainID, blockNum, err)
	}

	committeeIdentifiers := currentCommittee.IDs()

	logger.Debugf("Current committee: %v", committeeIdentifiers)

	committeeIDs := make(map[uint64]struct{})
	for _, id := range committeeIdentifiers {
		committeeIDs[uint64(id)] = struct{}{}
	}

	consensusMD := &smartbft_protos.ConfigMetadata{}
	if err := proto.Unmarshal(oc.ConsensusMetadata(), consensusMD); err != nil {
		logger.Panicf("[%s] Error getting consensus metadata while handling block with sequence number %d, due to %s", b.chainID, blockNum, err)
	}
	endpointsInCommittee := make(map[string]struct{})
	for _, consenter := range consensusMD.Consenters {
		if _, exists := committeeIDs[consenter.ConsenterId]; !exists {
			logger.Debugf("%d %s is not in the committee", consenter.ConsenterId, consenter.Host)
			continue
		}
		endpointsInCommittee[consenter.Host] = struct{}{}
	}
	logger.Debugf("Endpoints in committee: %v", endpointsInCommittee)

	if len(endpointsInCommittee) > 0 {
		var filteredEndpoints []comm.EndpointCriteria
		var filteredEndpointsURIs []string
		for _, ep := range endpoints {
			host, _, err := net.SplitHostPort(ep.Endpoint)
			if err != nil {
				logger.Warnf("Invalid host port string %s: %v", ep.Endpoint, err)
				continue
			}
			if _, exists := endpointsInCommittee[host]; !exists {
				continue
			}
			filteredEndpoints = append(filteredEndpoints, ep)
			filteredEndpointsURIs = append(filteredEndpointsURIs, ep.Endpoint)
		}
		endpoints = filteredEndpoints
		logger.Debugf("Filtering out endpoints of nodes not in the committee, remaining endpoints: %v", filteredEndpointsURIs)
	} else {
		logger.Debugf("Endpoints and consenter endpoints are disjoint, using the endpoints without filtering by committee")
	}

	return endpoints
}

func createGossipMsg(chainID string, payload *gossip_proto.Payload) *gossip_proto.GossipMessage {
	gossipMsg := &gossip_proto.GossipMessage{
		Nonce:   0,
		Tag:     gossip_proto.GossipMessage_CHAN_AND_ORG,
		Channel: []byte(chainID),
		Content: &gossip_proto.GossipMessage_DataMsg{
			DataMsg: &gossip_proto.DataMessage{
				Payload: payload,
			},
		},
	}
	return gossipMsg
}

func createPayload(seqNum uint64, marshaledBlock []byte) *gossip_proto.Payload {
	return &gossip_proto.Payload{
		Data:   marshaledBlock,
		SeqNum: seqNum,
	}
}

// computeBackOffDelay computes an exponential back-off delay and increments the counter, as long as the computed
// delay is below the maximal delay.
func computeBackOffDelay(count uint64) (time.Duration, uint64) {
	currentDelayNano := math.Pow(2.0, float64(count)) * float64(10*time.Millisecond.Nanoseconds())
	if currentDelayNano > float64(maxRetryDelay.Nanoseconds()) {
		return maxRetryDelay, count
	}
	return time.Duration(currentDelayNano), count + 1
}
