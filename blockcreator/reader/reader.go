package reader

import (
	"encoding/binary"
	makeblocks "github.com/hyperledger/fabric/blockcreator/proto"
	"io"
	"io/ioutil"
	"math"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
)

func IndexFromFile(file string) *makeblocks.Index {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}

	idx := &makeblocks.Index{}
	if err := proto.Unmarshal(b, idx); err != nil {
		panic(err)
	}
	return idx
}

type Index makeblocks.Index

func (index Index) Blocks() <-chan *common.Block {
	seq2blocks := make(map[uint64]chan *common.Block)
	seq2file := make(map[uint64]string)
	file2BlockChan := make(map[string]chan *common.Block)

	min := uint64(math.MaxUint64)
	max := uint64(0)
	for file, sequences := range index.FileToSequences {
		ch := make(chan *common.Block, 100)
		file2BlockChan[file] = ch
		for _, seq := range sequences.Seq {
			seq2file[seq] = file
			seq2blocks[seq] = ch

			if seq < min {
				min = seq
			}

			if seq > max {
				max = seq
			}
		}
	}

	populateBlockReads(index.FileToSequences, file2BlockChan)

	out := make(chan *common.Block, 1000)

	go func() {
		for seq := min; seq <= max; seq++ {
			in := seq2blocks[seq]
			out <- <-in
		}
	}()

	return out
}

func populateBlockReads(file2Seq map[string]*makeblocks.Sequences, file2BlockChan map[string]chan *common.Block) {
	for fName := range file2Seq {
		fd, err := os.Open(fName)
		if err != nil {
			panic(err)
		}
		br := &blockReader{
			f:    fd,
			buff: file2BlockChan[fName],
		}
		go br.Read()
	}
}

type blockReader struct {
	f              *os.File
	buff           chan<- *common.Block
	bytesReadSoFar int
}

func (br *blockReader) Read() {
	fi, err := br.f.Stat()
	if err != nil {
		panic(err)
	}

	totalFileSize := int(fi.Size())

	for br.bytesReadSoFar < totalFileSize {
		br.readBlock()
	}
}

func (br *blockReader) readBlock() {
	blockLenBytes := make([]byte, 4)
	_, err := io.ReadFull(br.f, blockLenBytes)
	if err != nil {
		panic(err)
	}
	blockSize := int(binary.BigEndian.Uint32(blockLenBytes))
	rawBlock := make([]byte, blockSize)
	_, err = io.ReadFull(br.f, rawBlock)
	if err != nil {
		panic(err)
	}

	br.bytesReadSoFar += 4 + blockSize

	block := &common.Block{}
	if err := proto.Unmarshal(rawBlock, block); err != nil {
		panic(err)
	}
	br.buff <- block
}
