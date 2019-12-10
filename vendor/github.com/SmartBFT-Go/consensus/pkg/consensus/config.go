// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import "time"

// Configuration defines the parameters needed in order to create an instance of Consensus.
type Configuration struct {
	// SelfID is the identifier of the node.
	SelfID uint64

	// RequestBatchMaxCount is the maximal number of requests in a batch.
	// A request batch that reaches this count is proposed immediately.
	RequestBatchMaxCount int
	// RequestBatchMaxBytes is the maximal total size of requests in a batch, in bytes.
	// This is also the maximal size of a request. A request batch that reaches this size is proposed immediately.
	RequestBatchMaxBytes uint64
	// RequestBatchMaxInterval is the maximal time interval a request batch is waiting before it is proposed.
	// A request batch is accumulating requests until RequestBatchMaxInterval had elapsed from the time the batch was
	// first created (i.e. the time the first request was added to it), or until it is of count RequestBatchMaxCount,
	// or total size RequestBatchMaxBytes, which ever happens first.
	RequestBatchMaxInterval time.Duration

	// IncomingMessageBufferSize is the size of the buffer holding incoming messages before they are processed.
	IncomingMessageBufferSize int
	// RequestPoolSize is the number of pending requests retained by the node.
	// The RequestPoolSize is recommended to be at least double (x2) the RequestBatchMaxCount.
	RequestPoolSize int

	// RequestTimeout is started from the moment a request is submitted, and defines the interval after which a request
	// is forwarded to the leader.
	RequestTimeout time.Duration
	// RequestLeaderFwdTimeout is started when RequestTimeout expires, and defines the interval after which the node
	// complains about the view leader.
	RequestLeaderFwdTimeout time.Duration
	// RequestAutoRemoveTimeout is started when RequestLeaderFwdTimeout expires, and defines the interval after which
	// a request is removed (dropped) from the request pool.
	RequestAutoRemoveTimeout time.Duration

	// ViewChangeResendInterval defined the interval in which the ViewChange message is resent.
	ViewChangeResendInterval time.Duration
	// ViewChangeTimeout is started when a node first receives a quorum of ViewChange messages, and defines the
	// interval after which the node will try to initiate a view change with a higher view number.
	ViewChangeTimeout time.Duration

	// LeaderHeartbeatTimeout is the interval after which, if nodes do not receive a "sign of life" from the leader,
	// they complain on the current leader and try to initiate a view change. A sign of life is either a heartbeat
	// or a message from the leader.
	LeaderHeartbeatTimeout time.Duration
	// LeaderHeartbeatCount is the number of heartbeats per LeaderHeartbeatTimeout that the leader should emit.
	// The heartbeat-interval is equal to: LeaderHeartbeatTimeout/LeaderHeartbeatCount.
	LeaderHeartbeatCount int

	// CollectTimeout is the interval after which the node stops listening to StateTransferResponse messages,
	// stops collecting information about view metadata from remote nodes.
	CollectTimeout time.Duration

	// SyncOnStart is a flag indicating whether a sync is required on startup.
	SyncOnStart bool

	// SpeedUpViewChange is a flag indicating whether a node waits for only f+1 view change messages to join
	// the view change (hence speeds up the view change process), or it waits for a quorum before joining.
	// Waiting only for f+1 is considered less safe.
	SpeedUpViewChange bool
}

// DefaultConfig contains reasonable values for a small cluster that resides on the same geography (or "Region"), but
// possibly on different availability zones within the geography. It is assumed that the typical latency between nodes,
// and between clients to nodes, is approximately 10ms.
// Set the SelfID.
var DefaultConfig = Configuration{
	RequestBatchMaxCount:      100,
	RequestBatchMaxBytes:      10 * 1024 * 1024,
	RequestBatchMaxInterval:   50 * time.Millisecond,
	IncomingMessageBufferSize: 200,
	RequestPoolSize:           400,
	RequestTimeout:            2 * time.Second,
	RequestLeaderFwdTimeout:   20 * time.Second,
	RequestAutoRemoveTimeout:  3 * time.Minute,
	ViewChangeResendInterval:  5 * time.Second,
	ViewChangeTimeout:         20 * time.Second,
	LeaderHeartbeatTimeout:    time.Minute,
	LeaderHeartbeatCount:      10,
	CollectTimeout:            time.Second,
	SyncOnStart:               false,
	SpeedUpViewChange:         false,
}
