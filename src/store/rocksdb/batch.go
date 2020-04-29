package rocksdb

import (
	"github.com/tecbot/gorocksdb"
)

// Batch .
type Batch struct {
	batch *gorocksdb.WriteBatch
}

// Set .
func (b *Batch) Set(key, val []byte) {
	b.batch.Put(key, val)
}

// Delete .
func (b *Batch) Delete(key []byte) {
	b.batch.Delete(key)
}

// Merge .
func (b *Batch) Merge(key, val []byte) {
	b.batch.Merge(key, val)
}

// Reset .
func (b *Batch) Reset() {
	b.batch.Clear()
}

// Count .
func (b *Batch) Count() int {
	return b.batch.Count()
}

// Close .
func (b *Batch) Close() error {
	b.batch.Destroy()
	b.batch = nil
	return nil
}
