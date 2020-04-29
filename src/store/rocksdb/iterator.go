package rocksdb

import (
	"bytes"

	"github.com/tecbot/gorocksdb"
)

// Iterator .
type Iterator struct {
	store    *Store
	iterator *gorocksdb.Iterator

	prefix []byte
	start  []byte
	end    []byte
}

// Seek .
func (i *Iterator) Seek(key []byte) {
	if i.start != nil && bytes.Compare(key, i.start) < 0 {
		key = i.start
	}
	if i.prefix != nil && !bytes.HasPrefix(key, i.prefix) {
		if bytes.Compare(key, i.prefix) < 0 {
			key = i.prefix
		} else {
			var end []byte
			for x := len(i.prefix) - 1; x >= 0; x-- {
				c := i.prefix[x]
				if c < 0xff {
					end = make([]byte, x+1)
					copy(end, i.prefix)
					end[x] = c + 1
					break
				}
			}
			key = end
		}
	}
	i.iterator.Seek(key)
}

// Next .
func (i *Iterator) Next() {
	i.iterator.Next()
}

// Current .
func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

// Key .
func (i *Iterator) Key() []byte {
	return i.iterator.Key().Data()
}

// Value .
func (i *Iterator) Value() []byte {
	return i.iterator.Value().Data()
}

// Valid .
func (i *Iterator) Valid() bool {
	if !i.iterator.Valid() {
		return false
	} else if i.prefix != nil && !bytes.HasPrefix(i.iterator.Key().Data(), i.prefix) {
		return false
	} else if i.end != nil && bytes.Compare(i.iterator.Key().Data(), i.end) >= 0 {
		return false
	}

	return true
}

// Close .
func (i *Iterator) Close() error {
	i.iterator.Close()
	return nil
}
