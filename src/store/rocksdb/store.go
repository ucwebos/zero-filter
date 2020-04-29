package rocksdb

import (
	"fmt"
	"os"

	"github.com/tecbot/gorocksdb"

	"zero-filter/src/store/kvstore"
)

// Name .
const Name = "rocksdb"

// Store .
type Store struct {
	path   string
	opts   *gorocksdb.Options
	config map[string]interface{}
	db     *gorocksdb.DB

	roptVerifyChecksums    bool
	roptVerifyChecksumsUse bool
	roptFillCache          bool
	roptFillCacheUse       bool
	roptReadTier           int
	roptReadTierUse        bool

	woptSync          bool
	woptSyncUse       bool
	woptDisableWAL    bool
	woptDisableWALUse bool
}

// New .
func New(config map[string]interface{}) (kvstore.KVStore, error) {

	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	if path == "" {
		return nil, os.ErrInvalid
	}

	rv := Store{
		path:   path,
		config: config,
		opts:   gorocksdb.NewDefaultOptions(),
	}

	_, err := applyConfig(rv.opts, config)
	if err != nil {
		return nil, err
	}

	b, ok := config["read_only"].(bool)
	if ok && b {
		rv.db, err = gorocksdb.OpenDbForReadOnly(rv.opts, rv.path, false)
	} else {
		rv.db, err = gorocksdb.OpenDb(rv.opts, rv.path)
	}

	if err != nil {
		return nil, err
	}

	b, ok = config["readoptions_verify_checksum"].(bool)
	if ok {
		rv.roptVerifyChecksums, rv.roptVerifyChecksumsUse = b, true
	}

	b, ok = config["readoptions_fill_cache"].(bool)
	if ok {
		rv.roptFillCache, rv.roptFillCacheUse = b, true
	}

	v, ok := config["readoptions_read_tier"].(float64)
	if ok {
		rv.roptReadTier, rv.roptReadTierUse = int(v), true
	}

	b, ok = config["writeoptions_sync"].(bool)
	if ok {
		rv.woptSync, rv.woptSyncUse = b, true
	}

	b, ok = config["writeoptions_disable_WAL"].(bool)
	if ok {
		rv.woptDisableWAL, rv.woptDisableWALUse = b, true
	}

	return &rv, nil
}

// Close .
func (s *Store) Close() error {
	s.db.Close()
	s.db = nil

	s.opts.Destroy()

	s.opts = nil

	return nil
}

// Reader .
func (s *Store) Reader() (kvstore.KVReader, error) {
	// snapshot := s.db.NewSnapshot()
	options := s.newReadOptions()
	// options.SetSnapshot(snapshot)
	return &Reader{
		store: s,
		// snapshot: snapshot,
		options: options,
	}, nil
}

// Writer .
func (s *Store) Writer() (kvstore.KVWriter, error) {
	return &Writer{
		store:     s,
		options:   s.newWriteOptions(),
		froptions: s.newFlushOptions(),
	}, nil
}

// Compact .
func (s *Store) Compact() error {
	s.db.CompactRange(gorocksdb.Range{})
	return nil
}
