/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package makeblocks

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
)

type Iterator struct {
	blocksLeftToIterate int
	closed              bool
	nextFile            string
	nextSeq             uint64
	files               map[string]*os.File
	positions           map[string]int
	idx                 *Index
}

func (i *Iterator) Next() *common.Block {
	if i.closed {
		panic("iterator has been already closed")
	}
	// Read the next block from the file
	f := i.files[i.nextFile]
	blockLengthBytes := make([]byte, 4)
	if _, err := f.Read(blockLengthBytes); err != nil {
		panic(err)
	}
	length := binary.BigEndian.Uint32(blockLengthBytes)
	blockBytes := make([]byte, length)
	if _, err := f.Read(blockBytes); err != nil {
		panic(err)
	}
	block := &common.Block{}
	if err := proto.Unmarshal(blockBytes, block); err != nil {
		panic(err)
	}
	if block.Header.Number != i.nextSeq {
		fmt.Printf("Expected %d but got %d instead\n", i.nextSeq, block.Header.Number)
		os.Exit(1)
	}
	i.blocksLeftToIterate--
	if i.blocksLeftToIterate == 0 {
		return nil
	}
	// Increment position for this file
	i.positions[i.nextFile]++
	// Increment next sequence to read
	var foundNextSeq bool
	i.nextSeq++
	// Find next file to read
	for file, sequences := range i.idx.FileToSequences {
		if i.positions[file] == len(sequences.Seq) {
			continue
		}
		if nextSeq := sequences.Seq[i.positions[file]]; nextSeq == i.nextSeq {
			i.nextFile = file
			foundNextSeq = true
			break
		}
	}
	if !foundNextSeq {
		panic(errors.New(fmt.Sprintf("could not find sequence %d in any file", i.nextSeq)))
	}

	return block
}

func (i *Iterator) Close() {
	if i.closed {
		return
	}
	for _, f := range i.files {
		f.Close()
	}
	i.closed = true
}

func (idx *Index) Iterator() *Iterator {
	firstFile, lowestSeq, totalBlockCount := idx.Info()
	i := &Iterator{
		blocksLeftToIterate: totalBlockCount,
		nextFile:            firstFile,
		nextSeq:             lowestSeq,
		files:               map[string]*os.File{},
		idx:                 idx,
		positions:           make(map[string]int),
	}
	var err error
	for file := range idx.FileToSequences {
		i.files[file], err = os.Open(file)
		if err != nil {
			panic(err)
		}
	}
	return i
}

func (idx *Index) Info() (firstFile string, lowestSeq uint64, totalBlockCount int) {
	// Figure out the first sequence
	var lowestSequence uint64
	fileWithFirstSeq := ""
	for file, sequences := range idx.FileToSequences {
		lowestSequence = sequences.Seq[0]
		fileWithFirstSeq = file
		break
	}

	var totalBlocks int
	for file, sequences := range idx.FileToSequences {
		totalBlocks += len(sequences.Seq)
		if lowestSequence > sequences.Seq[0] {
			fileWithFirstSeq = file
			lowestSequence = sequences.Seq[0]
		}
	}
	if fileWithFirstSeq == "" {
		panic("Didn't find file with first sequence")
	}
	firstFile = fileWithFirstSeq
	totalBlockCount = totalBlocks
	lowestSeq = lowestSequence
	return
}
