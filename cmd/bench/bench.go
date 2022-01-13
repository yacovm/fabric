/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	cs "github.com/SmartBFT-Go/randomcommittees"
	committee "github.com/SmartBFT-Go/randomcommittees/pkg"
	"github.com/hyperledger/fabric/cmd/common"
	comm "github.com/hyperledger/fabric/cmd/common/comm"
	"github.com/hyperledger/fabric/cmd/common/signer"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/consensus/smartbft"
	"github.com/hyperledger/fabric/orderer/consensus/smartbft/types"
	fcommon "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/orderer"
	protossmartbft "github.com/hyperledger/fabric/protos/orderer/smartbft"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"google.golang.org/grpc"
)

var (
	verbose = false
)

type Benchmark struct {
	Verbose                   *bool
	Endpoint                  *string
	Channel                   *string
	TPS                       *int
	WorkerNum                 *int
	workers                   []*worker
	lock                      sync.Mutex
	comm                      *comm.Client
	signer                    *signer.Signer
	logger                    *flogging.FabricLogger
	ledger                    *inMemLedger
	bf                        *blockFetcher
	lastCommitteeChangeSeq    uint64
	lastCommitteeChangeTime   int64
	committeeTransactionCount uint32
}

func (b *Benchmark) Run(config common.Config) error {
	b.validate()
	b.initialize(config)

	for {
		b.createWorkers()
		b.runWorkers()
	}

	return nil
}

func (b *Benchmark) onBlockCommit(block *fcommon.Block) {
	b.ledger.onCommit(block)
	md, err := types.CommitteeMetadataFromBlock(block)
	if err != nil {
		errExit("failed unmarshaling committee metadata: %v", err)
	}

	seq := block.Header.Number

	log("Received block %d", seq)

	lastCommitteeChangeSeq := atomic.LoadUint64(&b.lastCommitteeChangeSeq)
	if lastCommitteeChangeSeq == seq {
		return
	}

	atomic.AddUint32(&b.committeeTransactionCount, uint32(len(block.Data.Data)))

	if md == nil || md.CommitteeShiftAt != int64(seq) {
		return
	}

	log("Block %d changes committee", seq)
	atomic.StoreUint64(&b.lastCommitteeChangeSeq, seq)

	b.measureTPS(seq, lastCommitteeChangeSeq)

	b.stopWorkers()

	// OnBlockCommit is invoked from the block fetcher's
	// goroutine, and stop() waits or the goroutine to end.
	// Therefore we need to stop it from a different goroutine
	// otherwise we will be stuck.
	go func() {
		b.bf.stop()
		b.bf.start()
	}()
}

func (b *Benchmark) measureTPS(seq uint64, lastCommitteeChangeSeq uint64) {
	now := time.Now().UnixNano()

	defer func() {
		atomic.StoreUint32(&b.committeeTransactionCount, 0)
		atomic.StoreInt64(&b.lastCommitteeChangeTime, now)
	}()

	lastCommitteeChange := atomic.LoadInt64(&b.lastCommitteeChangeTime)
	if lastCommitteeChange == 0 {
		return
	}

	elapsed := time.Duration(now - lastCommitteeChange)
	tps := atomic.LoadUint32(&b.committeeTransactionCount) / uint32(elapsed.Seconds())
	fmt.Printf("Committee [%d --> %d] had average TPS of %d\n", lastCommitteeChangeSeq+1, seq, tps)
}

func (b *Benchmark) stopWorkers() {
	b.lock.Lock()
	defer b.lock.Unlock()

	for _, w := range b.workers {
		w.stop()
	}
}

func (b *Benchmark) createWorkers() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.workers = nil

	endpoints := b.currentCommitteeEndpoints()
	log("Endpoints of orderers in committee: %s", endpoints)
	for i := 0; i < *b.WorkerNum; i++ {
		w := &worker{
			id:         i,
			channel:    *b.Channel,
			stopSignal: make(chan struct{}),
			tps:        *b.TPS,
			endpoints:  endpoints,
			comm:       b.comm,
			signer:     b.signer,
		}
		w.running.Add(1)
		w.initialize()
		b.workers = append(b.workers, w)
	}
}

func (b *Benchmark) runWorkers() {
	var wg sync.WaitGroup
	wg.Add(len(b.workers))
	for _, w := range b.workers {
		go func(w *worker) {
			defer wg.Done()
			w.run()
		}(w)
	}

	wg.Wait()
}

func (b *Benchmark) currentCommitteeEndpoints() []string {
	consensusMD, endpoints, err := b.getConsensusMetadataAndEndpointsFromConfig()

	cr := &smartbft.CommitteeRetriever{
		NewCommitteeSelection: cs.NewCommitteeSelection,
		Logger:                b.logger,
		Ledger:                b.ledger,
	}

	nodes, err := cr.CurrentCommittee()
	if err != nil {
		errExit("failed computing current committee: %v", err)
	}

	return filterOutNonCommitteeNodes(nodes, consensusMD, endpoints)
}

func (b *Benchmark) getConsensusMetadataAndEndpointsFromConfig() (*protossmartbft.ConfigMetadata, []string, error) {
	lastBlock := b.ledger.Block(b.ledger.Height() - 1)
	obm := utils.GetOrdererblockMetadataOrPanic(lastBlock)
	var lastConfigIndex uint64
	if lastBlock.Header.Number != 0 {
		lastConfigIndex = obm.LastConfig.Index
	}

	configBlock := b.ledger.Block(lastConfigIndex)
	consensusMD, err := smartbft.ConsensusMDFromBlock(configBlock)
	if err != nil {
		errExit("failed extracting consensus configuration from block: %v", err)
	}

	bundle, err := channelconfig.NewBundleFromEnvelope(utils.UnmarshalEnvelopeOrPanic(configBlock.Data.Data[0]))
	if err != nil {
		errExit("failed creating bundle: %v", err)
	}

	var endpoints []string

	oc, _ := bundle.OrdererConfig()
	for _, org := range oc.Organizations() {
		endpoints = append(endpoints, org.Endpoints()...)
	}

	return consensusMD, endpoints, err
}

func (b *Benchmark) lastBlock(endpoint string) *fcommon.Block {
	env := b.makeDeliverEnvelope(last())
	block := fetchBlock(endpoint, b, env)
	return block
}

func (b *Benchmark) specificBlock(endpoint string, seq uint64) *fcommon.Block {
	b.logger.Infof("Requesting block %d from %s", seq, endpoint)
	env := b.makeDeliverEnvelope(specific(seq))
	block := fetchBlock(endpoint, b, env)
	return block
}

func fetchBlock(endpoint string, b *Benchmark, env *fcommon.Envelope) *fcommon.Block {
	conn, err := b.comm.NewConnection(endpoint, "")
	if err != nil {
		errExit("failed connecting to %s: %v", endpoint, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	deliverer, err := orderer.NewAtomicBroadcastClient(conn).Deliver(ctx)
	if err != nil {
		errExit("failed creating deliver stream to %s: %v", endpoint, err)
	}

	err = deliverer.Send(env)
	if err != nil {
		errExit("failed sending deliver envelope to %s: %v", endpoint, err)
	}

	resp, err := deliverer.Recv()
	if err != nil {
		errExit("failed receiving block from %s: %v", endpoint, err)
	}

	block := resp.GetBlock()
	if block == nil {
		errExit("Received %s from %s but expected a block", resp, endpoint)
	}
	return block
}

func (b *Benchmark) makeDeliverEnvelope(si *orderer.SeekInfo) *fcommon.Envelope {

	env, err := utils.CreateSignedEnvelope(
		fcommon.HeaderType_DELIVER_SEEK_INFO,
		*b.Channel,
		b.signer,
		si,
		0,
		0,
	)

	if err != nil {
		errExit("failed creating deliver envelope: %v", err)
	}

	return env
}

func (b *Benchmark) initialize(config common.Config) {
	if b.logger == nil {
		b.logger = flogging.MustGetLogger("bench")

	}
	signer, err := signer.NewSigner(config.SignerConfig)
	if err != nil {
		errExit("failed initializing signer: %v", err)
	}

	b.signer = signer

	commClient, err := comm.NewClient(config.TLSConfig)
	if err != nil {
		errExit("failed initializing communication: %v", err)
	}

	commClient.SetMaxRecvMsgSize(1024 * 1024 * 10)

	b.comm = commClient

	b.ledger = &inMemLedger{
		cache: make(map[uint64]*entry),
		backend: &remoteLedger{
			b: b,
		},
	}

	b.bf = &blockFetcher{
		stopChan:      make(chan struct{}),
		signer:        b.signer,
		comm:          b.comm,
		channel:       *b.Channel,
		getEndpoints:  b.currentCommitteeEndpoints,
		onBlockCommit: b.onBlockCommit,
	}
	b.bf.start()
}

func (b *Benchmark) validate() {
	if b.TPS == nil {
		errExit("must specify workCount argument")
	}
	if b.WorkerNum == nil {
		errExit("must specify workerNum argument")
	}
	if b.Channel == nil {
		errExit("must specify channel")
	}
	if b.Endpoint == nil {
		errExit("must specify server endpoint (host:port)")
	}
	if b.Verbose != nil {
		verbose = *b.Verbose
	}
}

type worker struct {
	id               int
	channel          string
	running          sync.WaitGroup
	streamAcks       sync.WaitGroup
	genTxns          sync.WaitGroup
	stopSignal       chan struct{}
	tps              int
	comm             *comm.Client
	signer           *signer.Signer
	endpoints        []string
	cancelFunc       func()
	broadcastStreams []orderer.AtomicBroadcast_BroadcastClient
	connections      []*grpc.ClientConn
}

func (w *worker) run() {
	defer w.running.Done()

	w.streamAcks.Add(len(w.broadcastStreams))
	w.ackStreams()
	defer w.streamAcks.Wait()

	txnBuff := make(chan *fcommon.Envelope, 1000)

	w.genTxns.Add(1)
	go w.generateTransactions(txnBuff)
	defer w.genTxns.Wait()

	sched := time.NewTicker(time.Second / time.Duration(w.tps))
	defer sched.Stop()

	for !w.shouldStop() {
		if w.sendBySchedule(sched, txnBuff) {
			return
		}
	}

}

func (w *worker) generateTransactions(txnBuff chan *fcommon.Envelope) {
	defer w.genTxns.Done()

	for !w.shouldStop() {
		select {
		case <-w.stopSignal:
			return
		// Try to enqueue or wait for space to be available
		case txnBuff <- w.makeTx():
		}
	}
}

func (w *worker) sendBySchedule(sched *time.Ticker, txnBuff chan *fcommon.Envelope) bool {
	select {
	case <-w.stopSignal:
		return true
	case <-sched.C:
		w.maybeSend(txnBuff)
	}
	return false
}

func (w *worker) maybeSend(txnBuff chan *fcommon.Envelope) {
	select {
	case <-w.stopSignal:
	case txn := <-txnBuff:
		w.sendTxn(txn)
	}
}

func (w *worker) makeTx() *fcommon.Envelope {
	env, err := utils.CreateSignedEnvelope(
		fcommon.HeaderType_ENDORSER_TRANSACTION,
		w.channel,
		w.signer,
		&peer.Transaction{Actions: []*peer.TransactionAction{
			{
				Payload: []byte{1, 2, 3},
				Header:  []byte{4, 5, 6},
			},
		}},
		0,
		0,
	)

	if err != nil {
		errExit("Failed creating broadcast envelope: %v", err)
	}

	return env
}
func (w *worker) shouldStop() bool {
	select {
	case <-w.stopSignal:
		return true
	default:

	}
	return false
}

func (w *worker) ackStreams() {
	for _, stream := range w.broadcastStreams {
		go w.ackStream(stream)
	}
}

func (w *worker) ackStream(stream orderer.AtomicBroadcast_BroadcastClient) {
	defer w.streamAcks.Done()

	for !w.shouldStop() {
		_, err := stream.Recv()
		if err != nil {
			if w.shouldStop() {
				return
			}
		}
	}
}

func (w *worker) sendTxn(env *fcommon.Envelope) {
	for _, stream := range w.broadcastStreams {
		stream.Send(env)
	}
}

func (w *worker) initialize() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancelFunc = cancel
	for _, endpoint := range w.endpoints {
		log("Worker %d is creating broadcasts stream to %s", w.id, endpoint)
		conn, err := w.comm.NewConnection(endpoint, "")
		if err != nil {
			errExit("failed connecting to %s: %v", endpoint, err)
		}
		stream, err := orderer.NewAtomicBroadcastClient(conn).Broadcast(ctx)
		if err != nil {
			errExit("failed creating broadcast stream to %s: %v", endpoint, err)
		}
		w.connections = append(w.connections, conn)
		w.broadcastStreams = append(w.broadcastStreams, stream)
	}
}

func (w *worker) stop() {
	select {
	case <-w.stopSignal:
		return
	default:

	}
	log("Worker %d shutting down", w.id)
	close(w.stopSignal)
	w.cancelFunc()
	w.running.Wait()
	for _, conn := range w.connections {
		conn.Close()
	}
}

type blockFetcher struct {
	running       sync.WaitGroup
	stopChan      chan struct{}
	cancel        func()
	onBlockCommit func(block *fcommon.Block)
	getEndpoints  func() []string
	comm          *comm.Client
	signer        *signer.Signer
	channel       string
}

func (bf *blockFetcher) start() {
	log("Starting block fetcher")
	bf.running.Add(1)
	bf.stopChan = make(chan struct{})
	endpoint, deliverStream := bf.connect()
	log("Block fetcher connected to %s", endpoint)
	go bf.run(endpoint, deliverStream)
}

func (bf *blockFetcher) run(endpoint string, deliverStream orderer.AtomicBroadcast_DeliverClient) {
	defer bf.running.Done()

	for !bf.shouldStop() {
		resp, err := deliverStream.Recv()
		if err != nil {
			if bf.shouldStop() {
				return
			}
			errExit("failed receiving block from %s: %v", endpoint, err)
		}

		if bf.shouldStop() {
			return
		}

		block := resp.GetBlock()
		if block == nil {
			errExit("Got %v from %s but expected a block", resp, endpoint)
		}

		bf.onBlockCommit(block)
	}
}

func (bf *blockFetcher) stop() {
	select {
	case <-bf.stopChan:
		return
	default:

	}

	log("Stopping block fetcher")

	close(bf.stopChan)
	if bf.cancel != nil {
		bf.cancel()
	}

	bf.running.Wait()
}

func (bf *blockFetcher) shouldStop() bool {
	select {
	case <-bf.stopChan:
		return true
	default:
		return false
	}
}

func (bf *blockFetcher) connect() (string, orderer.AtomicBroadcast_DeliverClient) {
	endpoints := bf.getEndpoints()
	// Choose a random endpoint to connect to
	endpoint := endpoints[rand.Intn(len(endpoints))]

	log("Block fetcher connecting to %s", endpoint)
	conn, err := bf.comm.NewConnection(endpoint, "")
	if err != nil {
		errExit("failed connecting to %s: %v", endpoint, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	bf.cancel = func() {
		cancel()
		log("Block fetcher closing connection to %s", endpoint)
		conn.Close()
		bf.cancel = nil
	}

	deliverer, err := orderer.NewAtomicBroadcastClient(conn).Deliver(ctx)
	if err != nil {
		errExit("failed creating deliver stream to %s: %v", endpoint, err)
	}

	env, err := utils.CreateSignedEnvelope(
		fcommon.HeaderType_DELIVER_SEEK_INFO,
		bf.channel,
		bf.signer,
		last(),
		0,
		0,
	)

	if err != nil {
		errExit("failed creating deliver envelope: %v", err)
	}

	err = deliverer.Send(env)
	if err != nil {
		errExit("failed sending deliver envelope to %s: %v", endpoint, err)
	}
	return endpoint, deliverer
}

const (
	maxInMemLedgerSize = 100
)

type inMemLedger struct {
	backend            smartbft.Ledger
	lock               sync.RWMutex
	timestamp          uint64
	cache              map[uint64]*entry
	lastCommittedBlock *fcommon.Block
}

func (inl *inMemLedger) Height() uint64 {
	inl.lock.RLock()
	lastBlock := inl.lastCommittedBlock
	inl.lock.RUnlock()

	if lastBlock != nil {
		return lastBlock.Header.Number + 1
	}

	height := inl.backend.Height()
	lastBlock = inl.backend.Block(height - 1)

	inl.lock.Lock()
	defer inl.lock.Unlock()

	inl.lastCommittedBlock = lastBlock

	return height
}

func (inl *inMemLedger) onCommit(block *fcommon.Block) {
	inl.lock.Lock()
	defer inl.lock.Unlock()

	if inl.lastCommittedBlock.Header.Number < block.Header.Number {
		inl.lastCommittedBlock = block
	}
}

func (inl *inMemLedger) Block(number uint64) *fcommon.Block {
	ts := atomic.AddUint64(&inl.timestamp, 1)

	inl.lock.RLock()
	item, exists := inl.cache[number]
	inl.lock.RUnlock()

	if exists {
		atomic.StoreUint64(item.timestamp, ts)
		return item.Block
	}

	block := inl.backend.Block(number)

	inl.lock.Lock()
	defer inl.lock.Unlock()

	inl.cache[number] = &entry{
		timestamp: &ts,
		Block:     block,
	}

	return block
}

func (inl *inMemLedger) maybeShrink() {
	inl.lock.RLock()
	shouldShrink := len(inl.cache) > maxInMemLedgerSize
	inl.lock.RUnlock()

	if !shouldShrink {
		return
	}

	inl.shrink()
}

func (inl *inMemLedger) shrink() {
	inl.lock.Lock()
	defer inl.lock.Unlock()

	for len(inl.cache) > maxInMemLedgerSize/10 {
		inl.shrinkLastUsed()
	}
}

func (inl *inMemLedger) shrinkLastUsed() {
	lastEntry := inl.first()
	lastTS := atomic.LoadUint64(lastEntry.timestamp)

	for _, entry := range inl.cache {
		currentTS := atomic.LoadUint64(entry.timestamp)
		if currentTS < lastTS {
			lastEntry = entry
			lastTS = currentTS
		}
	}

	delete(inl.cache, lastEntry.Header.Number)
}

func (inl *inMemLedger) first() *entry {
	for _, entry := range inl.cache {
		return entry
	}
	return nil
}

type entry struct {
	*fcommon.Block
	timestamp *uint64
}

type remoteLedger struct {
	b *Benchmark
}

func (r *remoteLedger) Height() uint64 {
	return r.b.lastBlock(*r.b.Endpoint).Header.Number + 1
}

func (r *remoteLedger) Block(seq uint64) *fcommon.Block {
	return r.b.specificBlock(*r.b.Endpoint, seq)
}

func errExit(format string, a ...interface{}) {
	fmt.Printf(fmt.Sprintf("%s\n", format), a...)
	os.Exit(2)
}

func log(format string, a ...interface{}) {
	if !verbose {
		return
	}
	fmt.Printf(fmt.Sprintf("%s\n", format), a...)
}

func last() *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Newest{Newest: &orderer.SeekNewest{}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}

func specific(seq uint64) *orderer.SeekInfo {
	specified := &orderer.SeekPosition{
		Type: &orderer.SeekPosition_Specified{
			Specified: &orderer.SeekSpecified{Number: seq},
		},
	}
	return &orderer.SeekInfo{
		Start:         specified,
		Stop:          specified,
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}

func filterOutNonCommitteeNodes(nodes committee.Nodes, consensusMD *protossmartbft.ConfigMetadata, totalEndpoints []string) []string {
	committeeIDs := make(map[uint64]struct{})

	for _, id := range nodes.IDs() {
		committeeIDs[uint64(id)] = struct{}{}
	}

	endpointsInCommittee := make(map[string]struct{})

	for _, c := range consensusMD.Consenters {
		if _, exists := committeeIDs[c.ConsenterId]; exists {
			endpointsInCommittee[c.Host] = struct{}{}
		}
	}

	var result []string
	for _, endpoint := range totalEndpoints {
		host, _, err := net.SplitHostPort(endpoint)
		if err != nil {
			errExit("failed splitting host port %s: %v", endpoint, err)
		}
		if _, exists := endpointsInCommittee[host]; !exists {
			continue
		}
		result = append(result, endpoint)
	}
	return result
}
