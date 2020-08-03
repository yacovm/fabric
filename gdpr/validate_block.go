package gdpr

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/queryresult"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protoutil"
)

func validate( data common.BlockData) error {

	//byte
	preimages := data.GetPreimageSpace()
	var writes [][]byte

	var hashes [][]byte
	for _, im := range preimages{
		hashes = append(hashes, Sha2(im))
	}
	//hashes is the set if hashes on the preimage space

	//preimages[][] = data.GetPreimageSpace()

	for seqInBlock, envBytes := range data {

		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			logger.Warning("Invalid envelope:", err)
			continue
		}

		// GAL: upto here - all good. need to extract pld now - find reference in other places in fabric
		payload, err := protoutil.UnmarshalPayload(env.Payload) //protoutil.GetBytesPayload()
		if err != nil {
			logger.Warning("Invalid payload:", err)
			continue
		}



		tx, err := protoutil.UnmarshalTransaction(payload.Data)

		_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
		//if err != nil {
		//	return nil, err
		//}

		txRWSet := &rwsetutil.TxRwSet{}
		txRWSet.FromProtoBytes(respPayload.Results)

		for _, nsRWSet := range txRWSet.NsRwSets {
			//nsRWSet.KvRwSet
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				writes = append(writes, kvWrite.GetValue())
			}
			/*if nsRWSet.NameSpace == namespace {
				// got the correct namespace, now find the key write
				for _, kvWrite := range nsRWSet.KvRwSet.Writes {
					if kvWrite.Key == key {
					}
				} // end keys loop
			} */// end if
		}


		cap, err := protoutil.UnmarshalChaincodeActionPayload(tx.Actions[0].Payload)

		tx2.GetActions()



		// ASSUMING hashes is a set of hashes on preimages, and assuming we have kvh the hash value foreach kvwrite,
		// all we need to do is to check that {kvh \in hashes} holds

		//cap.Action.ProposalResponsePayload
		//
		//prp, err := protoutil.UnmarshalProposalResponsePayload(cap.Action.ProposalResponsePayload)
		//
		//ca, err := protoutil.UnmarshalChaincodeAction(prp.Extension)
		//
		//ca.Response.Payload


	}
		return nil
	//data.D

	// foreach transaction tx in preImage space (Envelope.preimage)
	// Compare hashed data on block to hash(tx) -> data.data.

}
