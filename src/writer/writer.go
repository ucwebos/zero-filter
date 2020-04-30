package writer

import (
	"fmt"
	"time"

	"github.com/cstockton/go-conv"

	"zero-filter/src/config"
	"zero-filter/src/index"
	"zero-filter/src/store"
	"zero-filter/src/store/kvstore"
	"zero-filter/src/tools"
)

// Buckets .
var Buckets = make(map[string]*IWriter)

// IWriter .
type IWriter struct {
	kvWriter kvstore.KVWriter
	kvReader kvstore.KVReader
	bucket   *index.Bucket
	Chan     chan []map[string]interface{}
	Items    []map[string]interface{}
}

// NewIWriter .
func NewIWriter(bucket string) *IWriter {
	buk := index.GetBucket(bucket)
	kvWriter, err := store.KVWriter()
	if err != nil {
		panic(err)
	}
	kvReader, err := store.KVReader()
	if err != nil {
		panic(err)
	}
	iw := &IWriter{
		kvWriter: kvWriter,
		kvReader: kvReader,
		bucket:   buk,
		Chan:     make(chan []map[string]interface{}, 10),
		Items:    make([]map[string]interface{}, 0),
	}
	// 只开启一个写
	go func() {
		tick := time.NewTicker(1 * time.Second)
		for {
			select {
			case items := <-iw.Chan:
				for _, item := range items {
					iw.Items = append(iw.Items, item)
					if len(iw.Items) >= 50000 {
						iw.BatchSet()
						iw.Items = make([]map[string]interface{}, 0)
					}
				}
			case <-tick.C:
				if len(iw.Chan) == 0 && len(iw.Items) > 0 {
					iw.BatchSet()
					iw.Items = make([]map[string]interface{}, 0)
				}
			}
		}
	}()

	return iw
}

// Put .
func (w *IWriter) Put(data []map[string]interface{}) (ok bool) {
	w.Chan <- data
	return true
}

func (w *IWriter) toUKey32Map() (list map[uint32]map[string]interface{}, oKey32Map map[uint32][]byte) {
	list = make(map[uint32]map[string]interface{})
	oKey32Map = make(map[uint32][]byte)
	if !w.bucket.ModeUNI {
		for _, item := range w.Items {
			w.bucket.UKey32++
			uKey := w.bucket.UKey32
			list[uKey] = item
		}
		return
	}
	var (
		pKeys      = make([]string, 0)
		pKeyStrMap = make(map[string]map[string]interface{})
		pKvMap     = make(map[string]uint32)
	)
	for _, item := range w.Items {
		if pKey, ok := item[w.bucket.Primary]; ok {
			keyStr, _ := conv.String(pKey)
			pKeys = append(pKeys, keyStr)
			pKeyStrMap[keyStr] = item
		}
	}

	uKeyMap := w.bucket.IKeyMap.BatchGet(pKeys)
	for keyStr, vData := range pKeyStrMap {
		if uKey32, ok := uKeyMap[keyStr]; ok {
			list[uKey32] = vData
			oKey32Map[uKey32] = tools.Int64ToBytes(int64(uKey32))
		} else {
			w.bucket.UKey32++
			uKey32 := w.bucket.UKey32
			list[uKey32] = vData
			pKvMap[keyStr] = uKey32
		}
	}

	for k, uKeyb := range oKey32Map {
		buf, err := w.kvReader.Get(uKeyb)
		if err != nil {
			fmt.Println(err)
			// ...
			delete(oKey32Map, k)
			continue
		}
		oKey32Map[k] = buf
	}
	// 写主键map
	w.bucket.IKeyMap.BatchSet(pKvMap)
	return

}

// BatchSet 更新
func (w *IWriter) BatchSet() bool {
	list, oKey32Map := w.toUKey32Map()
	if list == nil || len(list) == 0 {
		return false
	}
	// 写索引
	for uKey, item := range list {
		// 判断是数据更新
		if oJSON, ok := oKey32Map[uKey]; ok {
			oData := make(map[string]interface{})
			err := tools.JSON.Unmarshal(oJSON, &oData)
			if err != nil {
				fmt.Println(err)
				//log ..
			}
			w.removeIdx(oData, uKey)
		}
		w.writeIdx(item, uKey)
	}
	// 写rocksdb
	w.writeBatchStore(list)
	// 写搜索
	if w.bucket.Search != nil {
		for uKey, vData := range list {
			for key := range w.bucket.SearchKeys {
				if val, ok := vData[key]; ok {
					vTxt, err := conv.String(val)
					if err != nil {
						continue
					}
					w.bucket.Search.Put(key, uKey, vTxt)
				}
			}
		}
	}
	return true
}

// Flush 清空所有数据
func (w *IWriter) Flush() {
	// todo ...
	w.kvWriter.Flush()
	w.bucket.Flush()
	// Search
	if w.bucket.Search != nil {
		for key := range w.bucket.SearchKeys {
			w.bucket.Search.Flush(key)
		}
	}
}

// writeBatchStore .
func (w *IWriter) writeBatchStore(vDatas map[uint32]map[string]interface{}) {
	wBatch := w.kvWriter.NewBatch()
	for uKey32, vData := range vDatas {
		buf, _ := tools.JSON.Marshal(vData)
		wBatch.Set(tools.Int64ToBytes(int64(uKey32)), buf)
	}
	w.kvWriter.ExecuteBatch(wBatch)
	wBatch.Close()
}

// writeIdx .
func (w *IWriter) removeIdx(vData map[string]interface{}, uKey32 uint32) {
	for k, v := range vData {
		tr := w.bucket.Btree(k)
		if tr == nil {
			continue
		}
		ibv := index.ToBVal(tr.KType, v)
		bv := tr.VGet(ibv)
		if bv == nil {
			continue
		}
		bv.SBMap.CheckedRemove(uKey32)
		if bv.SBMap.IsEmpty() {
			tr.Tr.Delete(bv)
		}
	}
}

// writeIdx .
func (w *IWriter) writeIdx(vData map[string]interface{}, uKey32 uint32) {
	for k, v := range vData {
		tr := w.bucket.Btree(k)
		if tr == nil {
			continue
		}
		ibv := index.ToBVal(tr.KType, v)
		bv := tr.VGet(ibv)
		if bv == nil {
			bv = tr.VPut(index.NewBVal(v))
		}
		bv.SBMap.Add(uKey32)
	}
}

// GetBucketWr .
func GetBucketWr(name string) *IWriter {
	if bt, ok := Buckets[name]; ok {
		return bt
	}
	return nil
}

// Init .
func Init() {
	for buk := range config.GConfig.Buckets {
		bucket := NewIWriter(buk)
		Buckets[buk] = bucket
	}
}
