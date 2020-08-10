package gdpr

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"

	//"crypto"
	"crypto/sha256"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
	//"hash"
)

var ErrVal = errors.New("gdpr: block validation failed")

func getHash(preimage []byte) []byte{
	h := sha256.New()
	return h.Sum(preimage)

}


func validate (block *common.Block) (*common.Block , error){
	preImages := block.GetData().GetPreimageSpace()

	//TODO: DBG ----------------
	preImages = extractPreimages(block)

	//TODO: DBG ----------------
	//var writes [][]byte
	var success = true

	//m :=
	m := map[string]bool{}
	var hashes [][]byte


	for _, im := range preImages {
		//im[0] = 0 TODO: toggle to fail test
		temp := getHash(im)
		hashes = append(hashes, temp )
		m[(string(temp))] = true
	}

	for _ , envBytes := range block.Data.Data {
		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			//logger.Warning("Invalid envelope:", err)
			continue
		}

		payload, err := protoutil.UnmarshalPayload(env.Payload) //protoutil.GetBytesPayload()
		if err != nil {
			//logger.Warning("Invalid payload:", err)
			continue
		}

		tx, err := protoutil.UnmarshalTransaction(payload.Data)
		if err != nil {
			return nil, err
		}

		_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
		if err != nil {
			return nil, err
		}

		txRWSet := &rwsetutil.TxRwSet{}
		//txRWSet.FromProtoBytes(respPayload.Results)

		if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
			return nil, err
		}

		for _, nsRWSet := range txRWSet.NsRwSets {
			//nsRWSet.KvRwSet
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				var a = kvWrite.GetValueHash()
				if a != nil{
					fmt.Printf("that's odd")
				}
				var b = getHash(kvWrite.GetValue())
				//kvWrite.ValueHash = []byte("Gal")
				if memberOf((string)(b),m) == false{
					success = false
				}

			}

		}
	}
	//TODO: if(!success, throw new error
	if !success{
		return nil, ErrVal
	}
	return nil, nil
}

func extractPreimages(block *common.Block) [][]byte {

	preimages := [][]byte{}

	for _, envBytes := range block.Data.Data {
		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			//logger.Warning("Invalid envelope:", err)
			continue
		}

		payload, err := protoutil.UnmarshalPayload(env.Payload) //protoutil.GetBytesPayload()
		if err != nil {
			//logger.Warning("Invalid payload:", err)
			continue
		}

		tx, err := protoutil.UnmarshalTransaction(payload.Data)
		if err != nil {
			return nil
		}

		_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
		if err != nil {
			return nil
		}

		txRWSet := &rwsetutil.TxRwSet{}
		//txRWSet.FromProtoBytes(respPayload.Results)

		if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
			return nil
		}

		for _, nsRWSet := range txRWSet.NsRwSets {
			//nsRWSet.KvRwSet
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				var a = kvWrite.GetValue()
				preimages = append(preimages, a)

				//kvWrite.ValueHash = []byte("Gal")

			}

		}

	}

	return preimages
}


func memberOf(a string, m map[string]bool) bool {
	if m[a] == true {
		return true
	}
	return false

}


func /*(block *common.Block)*/ validate_old(block *common.Block) (*common.Block , error) { //TODO: remove!

	//byte
	preImages := block.GetData().GetPreimageSpace()
	retBlock := common.Block{} // TODO: Does this actually create a block?

	retBlock.Data = block.GetData()
	retBlock.Header = block.GetHeader()
	retBlock.Metadata = block.GetMetadata()


	retBlock.GetData().PreimageSpace = [][]byte{} //TODO: Is this the way to replace the data there?

	//retBlock.GetData().Data = preImages

	var writes [][]byte
	h := sha256.New()

	var hashes [][]byte
	for _, im := range preImages {
		hashes = append(hashes, h.Sum(im))
	}
	//hashes is the set if hashes on the preimage space
	var success = true
	//preImages[][] = block.GetPreimageSpace()

	for _ , envBytes := range block.Data.Data {

		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			//logger.Warning("Invalid envelope:", err)
			//continue
			return nil, err
		}

		// GAL: upto here - all good. need to extract pld now - find reference in other places in fabric
		payload, err := protoutil.UnmarshalPayload(env.Payload) //protoutil.GetBytesPayload()
		if err != nil {
			return nil, err
		}

		tx, err := protoutil.UnmarshalTransaction(payload.Data)
		if err != nil {
			return nil, err
		}

		_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
		if err != nil {
			return nil, err
		}


		txRWSet := &rwsetutil.TxRwSet{}
		//txRWSet.FromProtoBytes(respPayload.Results)

		if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
			return nil, err
		}

		for _, nsRWSet := range txRWSet.NsRwSets {
			//nsRWSet.KvRwSet
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				var a = kvWrite.GetValueHash()
				kvWrite.ValueHash = []byte("Gal")
				if memberOf_old(a,hashes) == false{
					success = false
				}
				writes = append(writes,a) // kvWrite.GetValueHash())

			}

		}
	}
	if success == false {
		return nil, proto.ErrNil // TODO: Other error!
	} else {
		return nil, nil
	}


}

func memberOf_old(a []byte, b [][]byte) bool{
	for _,hashVal := range b{
		if bytes.Compare(a,hashVal) == 0 {
			return true
		}
	}
	return false

}