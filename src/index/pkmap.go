package index

import (
	"fmt"
	"zero-filter/src/config"
	"zero-filter/src/store/badger"
	"zero-filter/src/store/kvstore"
	"zero-filter/src/tools"
)

// PKMap .
type PKMap struct {
	store kvstore.KVStore
	r     kvstore.KVReader
	w     kvstore.KVWriter
}

// NewPKMap .
func NewPKMap(name string, partition int) (kfm *PKMap) {
	conf := map[string]interface{}{}
	conf["path"] = config.GConfig.Path + "/PK.DB"
	kvs, err := badger.New(conf)
	if err != nil {
		panic(err)
	}
	kvWriter, err := kvs.Writer()
	if err != nil {
		panic(err)
	}
	kvReader, err := kvs.Reader()
	if err != nil {
		panic(err)
	}
	return &PKMap{
		store: kvs,
		r:     kvReader,
		w:     kvWriter,
	}
}

// BatchSet .
func (km *PKMap) BatchSet(kvmap map[string]uint32) {
	b := km.w.NewBatch()
	for pk, uKey32 := range kvmap {
		b.Set([]byte(pk), tools.Int64ToBytes(int64(uKey32)))
	}
	km.w.ExecuteBatch(b)
	b.Close()
	return
}

// BatchGet .
func (km *PKMap) BatchGet(keys []string) (uKeyMap map[string]uint32) {
	kvmap, err := km.r.MultiGetMap(keys)
	if err != nil {
		fmt.Println(err)
		return
	}
	uKeyMap = make(map[string]uint32, 0)
	for k, v := range kvmap {
		uKeyMap[k] = uint32(tools.BytesToInt64(v))
	}
	return
}
