package smartbft

import (
	"encoding/hex"
	"fmt"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"sync"
)

type CertStore struct {
	db   *leveldb.DB
	lock sync.RWMutex
}

func newCertStore(path string) *CertStore {
	if _, err := fileutil.CreateDirIfMissing(path); err != nil {
		panic(fmt.Sprintf("could not create cert store folder at %s: %v", path, err))
	}
	db, err := leveldb.OpenFile(path, &opt.Options{ErrorIfMissing: false})
	if err != nil {
		panic(fmt.Sprintf("failed creating certificate store at %s: %v", path, err))
	}

	return &CertStore{db: db}
}

func (cs *CertStore) Lookup(key []byte) ([]byte, bool, error) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	k := []byte(hex.EncodeToString(key))
	val, err := cs.db.Get(k, &opt.ReadOptions{})
	if err == leveldb.ErrNotFound {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	return val, true, nil
}

func (cs *CertStore) Insert(keys, vals [][]byte) {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	if len(keys) != len(vals) {
		panic(fmt.Sprintf("tried to insert %d but %d values", len(keys), len(vals)))
	}

	b := leveldb.MakeBatch(len(keys))

	for i := 0; i < len(keys); i++ {
		k := []byte(hex.EncodeToString(keys[i]))

		_, err := cs.db.Get(k, &opt.ReadOptions{})
		if err != nil && err != leveldb.ErrNotFound {
			panic(fmt.Sprintf("failed looking up key: %v", err))
		}

		if err == nil {
			continue
		}

		b.Put(k, vals[i])
	}

	if err := cs.db.Write(b, &opt.WriteOptions{
		Sync: true,
	}); err != nil {
		panic(err)
	}
}
