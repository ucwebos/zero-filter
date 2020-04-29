package rocksdb

import (
	"fmt"

	"github.com/tecbot/gorocksdb"

	"zero-filter/src/store/kvstore"
)

// Writer .
type Writer struct {
	store     *Store
	options   *gorocksdb.WriteOptions
	froptions *gorocksdb.FlushOptions
}

// NewBatch .
func (w *Writer) NewBatch() kvstore.KVBatch {
	rv := Batch{
		batch: gorocksdb.NewWriteBatch(),
	}
	return &rv
}

// NewBatchEx .
func (w *Writer) NewBatchEx(options kvstore.KVBatchOptions) ([]byte, kvstore.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

// ExecuteBatch .
func (w *Writer) ExecuteBatch(b kvstore.KVBatch) error {
	batch, ok := b.(*Batch)
	if ok {
		return w.store.db.Write(w.options, batch.batch)
	}
	return fmt.Errorf("wrong type of batch")
}

// Flush .
func (w *Writer) Flush() error {
	w.store.db.Flush(w.froptions)
	return nil
}

// Close .
func (w *Writer) Close() error {
	w.options.Destroy()
	w.froptions.Destroy()
	return nil
}
