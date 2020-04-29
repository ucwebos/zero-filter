package store

import (
	"zero-filter/src/config"
	"zero-filter/src/store/kvstore"
	"zero-filter/src/store/rocksdb"
)

var kv kvstore.KVStore

// Init .
func Init() {
	conf := map[string]interface{}{
		"create_if_missing": true,
	}
	conf["path"] = config.GConfig.Path + "/KV.DB"
	kvs, err := rocksdb.New(conf)
	if err != nil {
		panic(err)
	}
	kv = kvs
}

// KVReader .
func KVReader() (kvstore.KVReader, error) {
	return kv.Reader()
}

// KVWriter .
func KVWriter() (kvstore.KVWriter, error) {
	return kv.Writer()
}
