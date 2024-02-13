package smartbft

import (
	"encoding/hex"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"os"
)

type CertStore struct {
	db *leveldb.DB
}

func newCertStore(path string) *CertStore {
	if err := os.MkdirAll(path, 0777); err != nil {
		panic(fmt.Sprintf("could not create cert store: %v", err))
	}
	db, err := leveldb.OpenFile(path, &opt.Options{})
	if err != nil {
		panic(fmt.Sprintf("failed creating certificate store: %v", err))
	}

	return &CertStore{db: db}
}

func (cs *CertStore) Lookup(key []byte) ([]byte, bool, error) {
	val, err := cs.db.Get([]byte(hex.EncodeToString(key)), &opt.ReadOptions{})
	if err == leveldb.ErrNotFound {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	return val, true, nil
}

func (cs *CertStore) Insert(key, val [][]byte) {
	cs.db.Write(&leveldb.Batch{}, &opt.WriteOptions{
		Sync: true,
	})
}