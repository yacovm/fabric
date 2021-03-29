/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft_test

import (
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric/orderer/consensus/smartbft"
	"github.com/hyperledger/fabric/orderer/consensus/smartbft/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

const (
	heartbeatTimeout  = 60 * time.Second
	heartbeatCount    = 10
	tickIncrementUnit = heartbeatTimeout / heartbeatCount
)

func TestNewHeartbeatMonitor(t *testing.T) {
	rpc := &mocks.RPC{}
	scheduler := make(chan time.Time)
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()
	hm := smartbft.NewHeartbeatMonitor(rpc, scheduler, logger, heartbeatTimeout, heartbeatCount, smartbft.HeartbeatReceiver, []uint64{1, 2, 3}, []uint64{11, 12, 13})
	assert.NotNil(t, hm)
	hm.Start()
	hm.Close()
}

func TestHeartbeatMonitorSender(t *testing.T) {
	rpc := &mocks.RPC{}
	var sendWG sync.WaitGroup
	sendWG.Add(3)
	rpc.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		sendWG.Done()
	}).Return(nil)
	scheduler := make(chan time.Time)
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()
	hm := smartbft.NewHeartbeatMonitor(rpc, scheduler, logger, heartbeatTimeout, heartbeatCount, smartbft.HeartbeatSender, []uint64{1, 2, 3}, []uint64{11, 12, 13})
	assert.NotNil(t, hm)
	hm.Start()
	rpc.AssertNotCalled(t, "SendConsensus", mock.Anything, mock.Anything)
	clock := fakeTime{}
	clock.advanceTime(2, scheduler)
	sendWG.Wait()
	rpc.AssertNumberOfCalls(t, "SendConsensus", 3)
	sendWG.Add(3)
	clock.advanceTime(1, scheduler)
	sendWG.Wait()
	rpc.AssertNumberOfCalls(t, "SendConsensus", 6)
	hm.Close()
}

func TestHeartbeatMonitorReceiver(t *testing.T) {
	rpc := &mocks.RPC{}
	var sendWG sync.WaitGroup
	sendWG.Add(3)
	scheduler := make(chan time.Time)
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()
	hm := smartbft.NewHeartbeatMonitor(rpc, scheduler, logger, heartbeatTimeout, heartbeatCount, smartbft.HeartbeatReceiver, []uint64{1, 2, 3}, []uint64{11, 12, 13})
	assert.NotNil(t, hm)
	hm.Start()
	assert.Equal(t, []uint64{1, 2, 3}, hm.GetSuspects()) // nobody sent any heartbeats yet and therefore all are suspects
	clock := fakeTime{}
	clock.advanceTime(1, scheduler)
	hm.ProcessHeartbeat(1)
	hm.ProcessHeartbeat(2)
	hm.ProcessHeartbeat(3)
	clock.advanceTime(1, scheduler)
	assert.Equal(t, []uint64{}, hm.GetSuspects())
	clock.advanceTime(10, scheduler)
	assert.Equal(t, []uint64{1, 2, 3}, hm.GetSuspects())
	clock.advanceTime(5, scheduler)
	hm.ProcessHeartbeat(2)
	clock.advanceTime(5, scheduler)
	assert.Equal(t, []uint64{1, 3}, hm.GetSuspects())
	clock.advanceTime(5, scheduler)
	hm.ProcessHeartbeat(1)
	hm.ProcessHeartbeat(3)
	clock.advanceTime(5, scheduler)
	assert.Equal(t, []uint64{2}, hm.GetSuspects())
	hm.Close()
}

type fakeTime struct {
	time time.Time
}

// each tick is (heartbeatTimeout / heartbeatCount)
func (t *fakeTime) advanceTime(ticks int, schedulers ...chan time.Time) {
	for i := 1; i <= ticks; i++ {
		newTime := t.time.Add(tickIncrementUnit)
		for _, scheduler := range schedulers {
			scheduler <- newTime
		}
		t.time = newTime
	}
}
