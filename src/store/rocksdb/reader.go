package rocksdb

import (
	"github.com/tecbot/gorocksdb"

	"zero-filter/src/store/kvstore"
)

// Reader .
type Reader struct {
	store *Store
	// snapshot *gorocksdb.Snapshot
	options *gorocksdb.ReadOptions
}

// Get .
func (r *Reader) Get(key []byte) ([]byte, error) {
	b, err := r.store.db.Get(r.options, key)
	if err != nil {
		return nil, err
	}
	return b.Data(), err
}

// MultiGet .
func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	vals := make([][]byte, 0)
	bs, err := r.store.db.MultiGet(r.options, keys...)
	if err != nil {
		return nil, err
	}
	for _, b := range bs {
		vals = append(vals, b.Data())
	}
	return vals, nil
}

// PrefixIterator .
func (r *Reader) PrefixIterator(prefix []byte) kvstore.KVIterator {
	rv := Iterator{
		store:    r.store,
		iterator: r.store.db.NewIterator(r.options),
		prefix:   prefix,
	}
	rv.Seek(prefix)
	return &rv
}

// RangeIterator .
func (r *Reader) RangeIterator(start, end []byte) kvstore.KVIterator {
	rv := Iterator{
		store:    r.store,
		iterator: r.store.db.NewIterator(r.options),
		start:    start,
		end:      end,
	}
	rv.Seek(start)
	return &rv
}

// Close .
func (r *Reader) Close() error {
	r.options.Destroy()
	// r.store.db.ReleaseSnapshot(r.snapshot)
	return nil
}
