package index

import (
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/cstockton/go-conv"
	"github.com/google/btree"

	"zero-filter/src/config"
)

// BVal .
type BVal struct {
	Val   interface{}
	SBMap *roaring.Bitmap
}

// Less .
func (b *BVal) Less(a btree.Item) bool {
	if ai, ok := a.(*BVal); ok {
		switch x := b.Val.(type) {
		case string:
			if as, ok := ai.Val.(string); ok {
				if o := strings.Compare(x, as); o == -1 {
					return true
				}
			}
		case int:
			if aii, ok := ai.Val.(int); ok {
				if x < aii {
					return true
				}
			}
		}
	}
	return false
}

// NewBVal .
func NewBVal(val interface{}) *BVal {
	var sbMap = roaring.New()
	return &BVal{
		Val:   val,
		SBMap: sbMap,
	}
}

// ToBVal .
func ToBVal(KType int, val interface{}) *BVal {
	vi := &BVal{}
	switch KType {
	case config.KTypeINT:
		ve, err := conv.Int(val)
		if err != nil {
			return nil
		}
		vi.Val = ve
	case config.KTypeString:
		ve, err := conv.String(val)
		if err != nil {
			return nil
		}
		vi.Val = ve
	}
	return vi
}
