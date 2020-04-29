package index

import (
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/google/btree"

	"zero-filter/src/config"
	"zero-filter/src/pb"
	"zero-filter/src/tools"
)

// MBTree .
type MBTree struct {
	TreeDir string
	KType   int
	Key     string
	FName   string
	Tr      *btree.BTree
}

// NewMBTree .
func NewMBTree(treeDir, key string, idx config.Idx) *MBTree {
	var (
		fName = treeDir + "/" + key
		tr    = btree.New(3)
		mtr   = &MBTree{
			TreeDir: treeDir,
			KType:   idx.KType,
			Key:     key,
			Tr:      tr,
			FName:   fName,
		}
	)
	mtr.Load()
	return mtr
}

// VGet .
func (t *MBTree) VGet(vi btree.Item) *BVal {
	item := t.Tr.Get(vi)
	if ai, ok := item.(*BVal); ok {
		return ai
	}
	return nil
}

// VPut .
func (t *MBTree) VPut(vi btree.Item) *BVal {
	// 写需要加锁
	t.Tr.ReplaceOrInsert(vi)
	if ai, ok := vi.(*BVal); ok {
		return ai
	}
	return nil
}

// Load .
func (t *MBTree) Load() (err error) {
	if tools.Exists(t.FName) {
		var (
			buf1, buf2 []byte
		)
		buf1, err = ioutil.ReadFile(t.FName)
		if err != nil {
			return
		}
		buf2, err = snappy.Decode(nil, buf1)
		if err != nil {
			return
		}
		t.FromBytes(buf2)
	}
	return
}

// Sync .
func (t *MBTree) Sync() (err error) {
	var (
		buf1, buf2 []byte
	)
	buf1, err = t.ToBytes()
	if err != nil {
		return
	}
	buf2 = snappy.Encode(nil, buf1)
	err = ioutil.WriteFile(t.FName, buf2, os.ModePerm)
	return
}

// ToBytes .
func (t *MBTree) ToBytes() (b []byte, err error) {
	out := make([]*pb.IdxVal, 0)
	t.Tr.Ascend(func(a btree.Item) bool {
		if ai, ok := a.(*BVal); ok {
			var (
				buf, _ = ai.SBMap.ToBytes()
				bvr    = &pb.IdxVal{
					Sbm: buf,
				}
			)
			switch x := ai.Val.(type) {
			case string:
				bvr.VStr = x
			case int:
				bvr.VInt = int64(x)
			}
			out = append(out, bvr)
		}
		return true
	})
	pbtree := &pb.IdxTree{
		List: out,
	}
	b, err = proto.Marshal(pbtree)
	return
}

// FromBytes .
func (t *MBTree) FromBytes(data []byte) (err error) {
	pbtree := &pb.IdxTree{}
	err = proto.Unmarshal(data, pbtree)
	if err != nil {
		return
	}
	for _, bvr := range pbtree.List {
		var bv *BVal
		switch t.KType {
		case config.KTypeINT:
			bv = NewBVal(bvr.VInt)
		case config.KTypeString:
			bv = NewBVal(bvr.VStr)
		}
		bv.SBMap.FromBuffer(bvr.Sbm)
		t.VPut(bv)
	}
	return
}
