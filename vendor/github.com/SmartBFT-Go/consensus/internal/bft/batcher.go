// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"time"
)

type BatchBuilder struct {
	pool         RequestPool
	maxMsgCount  int
	maxSizeBytes uint64
	batchTimeout time.Duration
	closeChan    chan struct{}
	closeLock    sync.Mutex // Reset and Close may be called by different threads
}

func NewBatchBuilder(pool RequestPool, maxMsgCount int, maxSizeBytes uint64, batchTimeout time.Duration) *BatchBuilder {
	b := &BatchBuilder{
		pool:         pool,
		maxMsgCount:  maxMsgCount,
		maxSizeBytes: maxSizeBytes,
		batchTimeout: batchTimeout,
		closeChan:    make(chan struct{}),
	}
	return b
}

// NextBatch returns the next batch of requests to be proposed.
// The method returns as soon as a the batch is full, in terms of request count or total size, or after a timeout.
// The method may block
func (b *BatchBuilder) NextBatch() [][]byte {
	currBatch, full := b.pool.NextRequests(b.maxMsgCount, b.maxSizeBytes)
	if full {
		return currBatch
	}

	timeout := time.After(b.batchTimeout) //TODO use task-scheduler based on logical time

	for {
		select {
		case <-b.closeChan:
			return nil
		case <-timeout:
			currBatch, _ = b.pool.NextRequests(b.maxMsgCount, b.maxSizeBytes)
			return currBatch
		default:
			time.Sleep(10 * time.Millisecond)
			if len(currBatch) < b.pool.Size() { // there is a possibility to extend the current batch
				currBatch, full = b.pool.NextRequests(b.maxMsgCount, b.maxSizeBytes)
				if full {
					return currBatch
				}
			}
		}
	}
}

// Close closes the close channel to stop NextBatch
func (b *BatchBuilder) Close() {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()
	select {
	case <-b.closeChan:
		return
	default:

	}
	close(b.closeChan)
}

func (b *BatchBuilder) Closed() bool {
	select {
	case <-b.closeChan:
		return true
	default:
		return false
	}
}

// Reset reopens the close channel to allow calling NextBatch
func (b *BatchBuilder) Reset() {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()
	b.closeChan = make(chan struct{})
}
