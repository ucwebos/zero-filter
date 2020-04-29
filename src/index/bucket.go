package index

import (
	"errors"
	"io/ioutil"
	"os"
	"time"

	"github.com/tidwall/wal"

	"zero-filter/src/config"
	"zero-filter/src/search"
	"zero-filter/src/tools"
)

// Buckets .
var Buckets = make(map[string]*Bucket)

// Bucket .
type Bucket struct {
	Name       string
	ModeUNI    bool
	Primary    string
	Idxs       map[string]config.Idx
	UKey32     uint32
	IKeyMap    *KFMap
	wal        *wal.Log
	BukDir     string
	TreeDir    string
	treeMap    map[string]*MBTree
	Search     *search.Search
	SearchKeys map[string]bool
}

// NewBucket .
func NewBucket(name string) (bt *Bucket, err error) {
	var (
		path     = config.GConfig.Path
		bukDir   = path + "/" + name
		walPath  = bukDir + "/WAL"
		treePath = bukDir + "/TREE"
	)
	buk, ok := config.GConfig.Buckets[name]
	if !ok {
		err = errors.New("error bucket name")
		return
	}
	if err = os.MkdirAll(bukDir, os.ModePerm); err != nil {
		return
	}
	wal, err := wal.Open(walPath, nil)
	if err != nil {
		return
	}
	if err = os.MkdirAll(treePath, os.ModePerm); err != nil {
		return
	}
	bt = &Bucket{
		Name:       name,
		wal:        wal,
		ModeUNI:    buk.ModeUNI,
		Primary:    buk.Primary,
		IKeyMap:    NewKFMap(name, buk.KFMapNum),
		UKey32:     0,
		BukDir:     bukDir,
		TreeDir:    treePath,
		Idxs:       buk.Idxs,
		treeMap:    make(map[string]*MBTree),
		SearchKeys: buk.Search,
	}

	if bt.ModeUNI && (bt.Primary == "") {
		err = errors.New("error not found bucket primary")
		return
	}
	bt.Load()

	// ...
	go func() {
		tick := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-tick.C:
				bt.Sync()
			}
		}
	}()

	return
}

// Btree .
func (bt *Bucket) Btree(key string) *MBTree {
	if tree, ok := bt.treeMap[key]; ok {
		return tree
	}
	return nil
}

// Sync 落地磁盘
func (bt *Bucket) Sync() {
	//uKey32
	var (
		uKeyFile = bt.BukDir + "/_UKEY32"
		buf      = tools.Int64ToBytes(int64(bt.UKey32))
	)
	err := ioutil.WriteFile(uKeyFile, buf, os.ModePerm)
	if err != nil {
		// log todo...
	}
	// idx
	for _, mbtree := range bt.treeMap {
		mbtree.Sync()
	}
}

// Flush .
func (bt *Bucket) Flush() (err error) {
	// todo ...
	return
}

// GetBucket .
func GetBucket(name string) *Bucket {
	if bt, ok := Buckets[name]; ok {
		return bt
	}
	return nil
}

// Load .
func (bt *Bucket) Load() {
	//uKey32
	var uKeyFile = bt.BukDir + "/_UKEY32"
	if tools.Exists(uKeyFile) {
		var (
			buf, _  = ioutil.ReadFile(uKeyFile)
			uKeyInt = tools.BytesToInt64(buf)
		)
		bt.UKey32 = uint32(uKeyInt)
	}
	//idx
	for key, idx := range bt.Idxs {
		mtr := NewMBTree(bt.TreeDir, key, idx)
		bt.treeMap[key] = mtr
	}
}

// Init 初始化
func Init() {
	for buk := range config.GConfig.Buckets {
		bucket, err := NewBucket(buk)
		if err != nil {
			panic(err)
		}
		Buckets[buk] = bucket
	}
}
