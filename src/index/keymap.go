package index

import (
	"github.com/RoaringBitmap/roaring"
)

// IKeyMap .
type IKeyMap struct {
	Len   int
	nodes map[rune][]*roaring.Bitmap
}

// NewKeyMap .
func NewKeyMap(length int) *IKeyMap {
	return &IKeyMap{
		Len:   length,
		nodes: make(map[rune][]*roaring.Bitmap),
	}
}

// Put .
func (im *IKeyMap) Put(key string, uKey32 uint32) {
	for i, rn := range key {
		if _, ok := im.nodes[rn]; !ok {
			im.nodes[rn] = make([]*roaring.Bitmap, im.Len)
		}
		var (
			bmArr = im.nodes[rn]
			bm    *roaring.Bitmap
		)
		bm = bmArr[i]
		if bm == nil {
			bm = roaring.New()
			bmArr[i] = bm
		}
		bm.Add(uKey32)
	}
}

// Get .
func (im *IKeyMap) Get(key string) (uKey32 uint32) {
	bms := make([]*roaring.Bitmap, 0)
	for i, rn := range key {
		bmArr, ok := im.nodes[rn]
		if !ok {
			return
		}
		bm := bmArr[i]
		if bm == nil {
			return
		}
		bms = append(bms, bm)
	}
	bm := roaring.FastAnd(bms...)
	bs := bm.ToArray()
	if len(bs) == 0 {
		return
	}
	return bs[0]
}

// ToJSON .
func (im *IKeyMap) ToJSON() (b []byte, err error) {
	// for k, mbm := range ir.nodes {
	// }
	return
}

// FromJSON .
func (im *IKeyMap) FromJSON() (err error) {
	return
}
