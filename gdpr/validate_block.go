package gdpr

import (
	"bytes"
	"github.com/gogo/protobuf/proto"

	//"crypto"
	"crypto/sha256"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
	//"hash"
)

func /*(block *common.Block)*/ validate(block *common.Block) (*common.Block , error) {

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
			continue
		}

		// GAL: upto here - all good. need to extract pld now - find reference in other places in fabric
		payload, err := protoutil.UnmarshalPayload(env.Payload) //protoutil.GetBytesPayload()
		if err != nil {
			//logger.Warning("Invalid payload:", err)
			continue
		}

		tx, err := protoutil.UnmarshalTransaction(payload.Data)

		_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])


		txRWSet := &rwsetutil.TxRwSet{}
		txRWSet.FromProtoBytes(respPayload.Results)

		for _, nsRWSet := range txRWSet.NsRwSets {
			//nsRWSet.KvRwSet
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				var a = kvWrite.GetValueHash()
				if memberOf(a,hashes) == false{
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

func memberOf(a []byte, b [][]byte) bool{
	for _,hashVal := range b{
		if bytes.Compare(a,hashVal) == 0 {
			return true
		}
	}
	return false

}