package badger

import (
	"fmt"

	"zero-filter/src/store/kvstore"
)

// Writer implements bleve store Writer interface
type Writer struct {
	store *Store
}

// NewBatch implements NewBatch
func (w *Writer) NewBatch() kvstore.KVBatch {
	rv := Batch{
		batch: w.store.db.NewWriteBatch(),
	}
	return &rv
}

// NewBatchEx .
func (w *Writer) NewBatchEx(options kvstore.KVBatchOptions) ([]byte, kvstore.KVBatch, error) {
	return nil, w.NewBatch(), nil
}

// ExecuteBatch .
func (w *Writer) ExecuteBatch(b kvstore.KVBatch) (err error) {
	batch, ok := b.(*Batch)
	if ok {
		return batch.batch.Flush()
	}
	return fmt.Errorf("wrong type of batch")
}

// Flush .
func (w *Writer) Flush() error {
	return nil
}

// Close .
func (w *Writer) Close() error {
	return nil
}
