// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"sort"
	"sync/atomic"
	"time"

	algorithm "github.com/SmartBFT-Go/consensus/internal/bft"
	bft "github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/pkg/errors"
)

// Consensus submits requests to be total ordered,
// and delivers to the application proposals by invoking Deliver() on it.
// The proposals contain batches of requests assembled together by the Assembler.
type Consensus struct {
	Config            Configuration
	Application       bft.Application
	Assembler         bft.Assembler
	WAL               bft.WriteAheadLog
	WALInitialContent [][]byte
	Comm              bft.Comm
	Signer            bft.Signer
	Verifier          bft.Verifier
	RequestInspector  bft.RequestInspector
	Synchronizer      bft.Synchronizer
	Logger            bft.Logger
	Metadata          protos.ViewMetadata
	LastProposal      types.Proposal
	LastSignatures    []types.Signature
	Scheduler         <-chan time.Time
	ViewChangerTicker <-chan time.Time

	viewChanger   *algorithm.ViewChanger
	controller    *algorithm.Controller
	collector     *algorithm.StateCollector
	state         *algorithm.PersistedState
	numberOfNodes uint64
	nodes         []uint64
}

func (c *Consensus) Complain(viewNum uint64, stopView bool) {
	c.viewChanger.StartViewChange(viewNum, stopView)
}

func (c *Consensus) Deliver(proposal types.Proposal, signatures []types.Signature) {
	c.Application.Deliver(proposal, signatures)
}

func (c *Consensus) Start() error {
	if err := c.validateConfiguration(); err != nil {
		return errors.Wrapf(err, "configuration is invalid")
	}
	nodes := c.Comm.Nodes()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i] < nodes[j]
	})
	c.nodes = nodes
	c.numberOfNodes = uint64(len(c.nodes))
	inFlight := algorithm.InFlightData{}

	c.state = &algorithm.PersistedState{
		InFlightProposal: &inFlight,
		Entries:          c.WALInitialContent,
		Logger:           c.Logger,
		WAL:              c.WAL,
	}

	cpt := types.Checkpoint{}
	cpt.Set(c.LastProposal, c.LastSignatures)

	c.viewChanger = &algorithm.ViewChanger{
		SelfID:            c.Config.SelfID,
		N:                 c.numberOfNodes,
		NodesList:         c.nodes,
		SpeedUpViewChange: c.Config.SpeedUpViewChange,
		Logger:            c.Logger,
		Signer:            c.Signer,
		Verifier:          c.Verifier,
		Application:       c,
		Checkpoint:        &cpt,
		InFlight:          &inFlight,
		State:             c.state,
		// Controller later
		// RequestsTimer later
		Ticker:            c.ViewChangerTicker,
		ResendTimeout:     c.Config.ViewChangeResendInterval,
		ViewChangeTimeout: c.Config.ViewChangeTimeout,
		InMsqQSize:        c.Config.IncomingMessageBufferSize,
	}

	c.collector = &algorithm.StateCollector{
		SelfID:         c.Config.SelfID,
		N:              c.numberOfNodes,
		Logger:         c.Logger,
		CollectTimeout: c.Config.CollectTimeout,
	}

	c.controller = &algorithm.Controller{
		Checkpoint:       &cpt,
		WAL:              c.WAL,
		ID:               c.Config.SelfID,
		N:                c.numberOfNodes,
		NodesList:        c.nodes,
		Verifier:         c.Verifier,
		Logger:           c.Logger,
		Assembler:        c.Assembler,
		Application:      c,
		FailureDetector:  c,
		Synchronizer:     c.Synchronizer,
		Comm:             c.Comm,
		Signer:           c.Signer,
		RequestInspector: c.RequestInspector,
		ViewChanger:      c.viewChanger,
		ViewSequences:    &atomic.Value{},
		Collector:        c.collector,
		State:            c.state,
	}

	c.viewChanger.Comm = c.controller
	c.viewChanger.Synchronizer = c.controller

	c.controller.ProposerBuilder = c.proposalMaker()

	opts := algorithm.PoolOptions{
		QueueSize:         int64(c.Config.RequestPoolSize),
		RequestTimeout:    c.Config.RequestTimeout,
		LeaderFwdTimeout:  c.Config.RequestLeaderFwdTimeout,
		AutoRemoveTimeout: c.Config.RequestAutoRemoveTimeout,
	}
	submittedChan := make(chan struct{}, 1)
	pool := algorithm.NewPool(c.Logger, c.RequestInspector, c.controller, opts, submittedChan)
	batchBuilder := algorithm.NewBatchBuilder(pool, submittedChan, c.Config.RequestBatchMaxCount, c.Config.RequestBatchMaxBytes, c.Config.RequestBatchMaxInterval)
	leaderMonitor := algorithm.NewHeartbeatMonitor(c.Scheduler, c.Logger, c.Config.LeaderHeartbeatTimeout, c.Config.LeaderHeartbeatCount, c.controller, c.numberOfNodes, c.controller, c.controller.ViewSequences)
	c.controller.RequestPool = pool
	c.controller.Batcher = batchBuilder
	c.controller.LeaderMonitor = leaderMonitor

	c.viewChanger.Controller = c.controller
	c.viewChanger.Pruner = c.controller
	c.viewChanger.RequestsTimer = pool
	c.viewChanger.ViewSequences = c.controller.ViewSequences

	view := c.Metadata.ViewId
	seq := c.Metadata.LatestSequence

	viewChange, err := c.state.LoadViewChangeIfApplicable()
	if err != nil {
		c.Logger.Panicf("Failed loading view change, error: %v", err)
	}
	if viewChange == nil {
		c.Logger.Debugf("No view change to restore")
	} else {
		// Check if the application has a newer view
		if viewChange.NextView >= c.Metadata.ViewId {
			c.Logger.Debugf("Restoring from view change with view %d, while application has view %d and seq %d", viewChange.NextView, c.Metadata.ViewId, c.Metadata.LatestSequence)
			view = viewChange.NextView
			restoreChan := make(chan struct{}, 1)
			restoreChan <- struct{}{}
			c.viewChanger.Restore = restoreChan
		}
	}

	viewSeq, err := c.state.LoadNewViewIfApplicable()
	if err != nil {
		c.Logger.Panicf("Failed loading new view, error: %v", err)
	}
	if viewSeq == nil {
		c.Logger.Debugf("No new view to restore")
	} else {
		// Check if metadata should be taken from the restored new view or from the application
		if viewSeq.Seq >= c.Metadata.LatestSequence {
			c.Logger.Debugf("Restoring from new view with view %d and seq %d, while application has view %d and seq %d", viewSeq.View, viewSeq.Seq, c.Metadata.ViewId, c.Metadata.LatestSequence)
			view = viewSeq.View
			seq = viewSeq.Seq
		}
	}

	// If we delivered to the application proposal with sequence i,
	// then we are expecting to be proposed a proposal with sequence i+1.
	c.collector.Start()
	c.viewChanger.Start(view)
	c.controller.Start(view, seq+1, c.Config.SyncOnStart)
	return nil
}

func (c *Consensus) Stop() {
	c.viewChanger.Stop()
	c.controller.Stop()
	c.collector.Stop()
}

func (c *Consensus) HandleMessage(sender uint64, m *protos.Message) {
	c.controller.ProcessMessages(sender, m)
}

func (c *Consensus) HandleRequest(sender uint64, req []byte) {
	c.controller.HandleRequest(sender, req)
}

func (c *Consensus) SubmitRequest(req []byte) error {
	c.Logger.Debugf("Submit Request: %s", c.RequestInspector.RequestID(req))
	return c.controller.SubmitRequest(req)
}

func (c *Consensus) proposalMaker() *algorithm.ProposalMaker {
	return &algorithm.ProposalMaker{
		State:           c.state,
		Comm:            c.controller,
		Decider:         c.controller,
		Logger:          c.Logger,
		Signer:          c.Signer,
		SelfID:          c.Config.SelfID,
		Sync:            c.controller,
		FailureDetector: c,
		Verifier:        c.Verifier,
		N:               c.numberOfNodes,
		InMsqQSize:      c.Config.IncomingMessageBufferSize,
		ViewSequences:   c.controller.ViewSequences,
	}
}

func (c *Consensus) validateConfiguration() error {
	if !(c.Config.SelfID > 0) {
		return errors.Errorf("SelfID is lower than or equal to zero")
	}
	nodes := c.Comm.Nodes()
	for _, val := range nodes {
		if val == 0 {
			return errors.Errorf("node id 0 is not permitted")
		}
	}
	if !(c.Config.RequestBatchMaxCount > 0) {
		return errors.Errorf("RequestBatchMaxCount should be greater than zero")
	}
	if !(c.Config.RequestBatchMaxBytes > 0) {
		return errors.Errorf("RequestBatchMaxBytes should be greater than zero")
	}
	if !(c.Config.RequestBatchMaxInterval > 0) {
		return errors.Errorf("RequestBatchMaxInterval should be greater than zero")
	}
	if !(c.Config.IncomingMessageBufferSize > 0) {
		return errors.Errorf("IncomingMessageBufferSize should be greater than zero")
	}
	if !(c.Config.RequestPoolSize > 0) {
		return errors.Errorf("RequestPoolSize should be greater than zero")
	}
	if !(c.Config.RequestTimeout > 0) {
		return errors.Errorf("RequestTimeout should be greater than zero")
	}
	if !(c.Config.RequestLeaderFwdTimeout > 0) {
		return errors.Errorf("RequestLeaderFwdTimeout should be greater than zero")
	}
	if !(c.Config.RequestAutoRemoveTimeout > 0) {
		return errors.Errorf("RequestAutoRemoveTimeout should be greater than zero")
	}
	if !(c.Config.ViewChangeResendInterval > 0) {
		return errors.Errorf("ViewChangeResendInterval should be greater than zero")
	}
	if !(c.Config.ViewChangeTimeout > 0) {
		return errors.Errorf("ViewChangeTimeout should be greater than zero")
	}
	if !(c.Config.LeaderHeartbeatTimeout > 0) {
		return errors.Errorf("LeaderHeartbeatTimeout should be greater than zero")
	}
	if !(c.Config.LeaderHeartbeatCount > 0) {
		return errors.Errorf("LeaderHeartbeatCount should be greater than zero")
	}
	if !(c.Config.CollectTimeout > 0) {
		return errors.Errorf("CollectTimeout should be greater than zero")
	}
	if uint64(c.Config.RequestBatchMaxCount) > c.Config.RequestBatchMaxBytes {
		return errors.Errorf("RequestBatchMaxCount is bigger than RequestBatchMaxBytes")
	}
	if c.Config.RequestTimeout > c.Config.RequestLeaderFwdTimeout {
		return errors.Errorf("RequestTimeout is bigger than RequestLeaderFwdTimeout")
	}
	if c.Config.RequestLeaderFwdTimeout > c.Config.RequestAutoRemoveTimeout {
		return errors.Errorf("RequestLeaderFwdTimeout is bigger than RequestAutoRemoveTimeout")
	}
	if c.Config.ViewChangeResendInterval > c.Config.ViewChangeTimeout {
		return errors.Errorf("ViewChangeResendInterval is bigger than ViewChangeTimeout")
	}
	return nil
}
