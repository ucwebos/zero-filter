package entry

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/cstockton/go-conv"
	"github.com/google/btree"

	"zero-filter/src/index"
)

// Query .
type Query struct {
	Bucket    string        `json:"bucket"` //bucket
	BucketObj *index.Bucket `json:"-"`
	Where     []WhereOp     `json:"where"`           //条件
	Limit     *Limit        `json:"limit,omitempty"` //分页
	Count     bool          `json:"count"`           //只查数量
	Stream    bool          `json:"stream"`          //流式分块查询
	StreamID  string        `json:"streamId"`        //流式分块查询ID
}

// QueryResp .
type QueryResp struct {
	List      []json.RawMessage `json:"list,omitempty"`
	Size      int               `json:"size"`
	StreamEnd bool              `json:"streamEnd,omitempty"`
	StreamID  string            `json:"streamId,omitempty"`
}

const (
	// OpTypeIn .
	OpTypeIn = "in"
	// OpTypeNot .
	OpTypeNot = "!="
	// OpTypeLess .
	OpTypeLess = "<"
	// OpTypeLessEq .
	OpTypeLessEq = "<="
	// OpTypeEq .
	OpTypeEq = "="
	// OpTypeMoreEq .
	OpTypeMoreEq = ">="
	// OpTypeMore .
	OpTypeMore = ">"
	// OpTypeLike .
	OpTypeLike = "like"
)

// WhereOp .
type WhereOp struct {
	Or  []WhereOp   `json:"or,omitempty"`
	And []WhereOp   `json:"and,omitempty"`
	Key string      `json:"key"`
	Op  string      `json:"op"`
	Val interface{} `json:"val"`
}

// Limit .
type Limit struct {
	Start int `json:"start"`
	Size  int `json:"size"`
}

// Exec .
func (q *Query) Exec() (bm *roaring.Bitmap, err error) {
	bucket := index.GetBucket(q.Bucket)
	if bucket == nil {
		err = fmt.Errorf("error bucket[%s]", q.Bucket)
		return
	}
	q.BucketObj = bucket
	bm = q.and(q.Where)
	return
}

func (q *Query) itemExec(whr WhereOp) (bm *roaring.Bitmap) {
	if whr.Or != nil && len(whr.Or) > 0 {
		return q.or(whr.Or)
	}
	if whr.And != nil && len(whr.Or) > 0 {
		return q.and(whr.Or)
	}
	idxs := q.BucketObj.Idxs
	//判断异常条件 & 条件转换为索引对应的类型
	idx, ok := idxs[whr.Key]
	if !ok && whr.Op != OpTypeLike {
		return
	}
	bv := index.ToBVal(idx.KType, whr.Val)
	if bv == nil && whr.Op != OpTypeLike {
		return
	}

	switch whr.Op {
	case OpTypeEq:
		return q.findEq(whr.Key, bv)
	case OpTypeIn:
		return q.findIn(whr.Key, bv)
	case OpTypeLess:
		return q.findLessThan(whr.Key, bv)
	case OpTypeLessEq:
		return q.findLessOrEq(whr.Key, bv)
	case OpTypeMore:
		return q.findMoreThan(whr.Key, bv)
	case OpTypeMoreEq:
		return q.findMoreOrEq(whr.Key, bv)
	case OpTypeNot:
		return q.findNot(whr.Key, bv)
	case OpTypeLike:
		return q.findLike(whr.Key, whr.Val)
	}
	return
}

func (q *Query) and(whrs []WhereOp) (bm *roaring.Bitmap) {
	bm = nil
	for _, whr := range whrs {
		sb := q.itemExec(whr)
		if sb == nil && whr.Op != OpTypeNot {
			return
		}
		if bm == nil {
			bm = sb.Clone()
			continue
		}
		// andnot ...
		if whr.Op == OpTypeNot && sb != nil {
			bm = roaring.AndNot(bm, sb)
		} else {
			bm = roaring.And(bm, sb)
		}
	}
	// todo 优化 FastAnd ？
	return
}

func (q *Query) or(whrs []WhereOp) (bm *roaring.Bitmap) {
	bms := make([]*roaring.Bitmap, 0)
	for _, whr := range whrs {
		sb := q.itemExec(whr)
		bms = append(bms, sb)
	}
	return or(bms...)
}

func or(bms ...*roaring.Bitmap) (bm *roaring.Bitmap) {
	bmsr := make([]*roaring.Bitmap, 0)
	for _, b := range bms {
		if b != nil {
			bmsr = append(bmsr, b)
		}
	}
	if len(bmsr) == 0 {
		return
	}
	return roaring.FastOr(bmsr...)
}

func (q *Query) findNot(key string, val interface{}) (bm *roaring.Bitmap) {
	return or(q.findLessThan(key, val), q.findLessThan(key, val))
}

func (q *Query) findIn(key string, val interface{}) (bm *roaring.Bitmap) {
	var list []interface{}
	if reflect.TypeOf(val).Kind() == reflect.Slice {
		s := reflect.ValueOf(val)
		for i := 0; i < s.Len(); i++ {
			ele := s.Index(i)
			list = append(list, ele.Interface())
		}
	}
	bms := make([]*roaring.Bitmap, 0)
	for _, v := range list {
		b := q.findEq(key, v)
		bms = append(bms, b)
	}
	return or(bms...)
}

func (q *Query) findLessThan(key string, val interface{}) (bm *roaring.Bitmap) {
	if vi, ok := val.(*index.BVal); ok {
		tr := q.BucketObj.Btree(key)
		bms := make([]*roaring.Bitmap, 0)
		tr.Tr.AscendLessThan(vi, func(a btree.Item) bool {
			if bv, ok := a.(*index.BVal); ok {
				bms = append(bms, bv.SBMap)
			}
			return true
		})
		return or(bms...)
	}
	return nil
}

func (q *Query) findLessOrEq(key string, val interface{}) (bm *roaring.Bitmap) {
	if vi, ok := val.(*index.BVal); ok {
		tr := q.BucketObj.Btree(key)
		bms := make([]*roaring.Bitmap, 0)
		tr.Tr.AscendLessThan(vi, func(a btree.Item) bool {
			if bv, ok := a.(*index.BVal); ok {
				bms = append(bms, bv.SBMap)
			}
			return true
		})
		bv := tr.VGet(vi)
		if bv != nil {
			bms = append(bms, bv.SBMap)
		}
		return or(bms...)
	}
	return nil
}

func (q *Query) findMoreThan(key string, val interface{}) (bm *roaring.Bitmap) {
	bm = q.findMoreOrEq(key, val)
	bmn := q.findEq(key, val)
	if bmn != nil && bm != nil {
		bm = roaring.AndNot(bm, bmn)
	}
	return
}

func (q *Query) findMoreOrEq(key string, val interface{}) (bm *roaring.Bitmap) {
	if vi, ok := val.(*index.BVal); ok {
		tr := q.BucketObj.Btree(key)
		bms := make([]*roaring.Bitmap, 0)
		tr.Tr.AscendGreaterOrEqual(vi, func(a btree.Item) bool {
			if bv, ok := a.(*index.BVal); ok {
				bms = append(bms, bv.SBMap)
			}
			return true
		})
		return or(bms...)
	}
	return nil
}

func (q *Query) findLike(key string, val interface{}) (bm *roaring.Bitmap) {
	// 如果没有初始化搜索
	fmt.Println(q.BucketObj.Search)
	if q.BucketObj.Search == nil {
		return
	}
	vStr, err := conv.String(val)
	if err != nil {
		return
	}
	return q.BucketObj.Search.Search(key, vStr)
}

func (q *Query) findEq(key string, val interface{}) *roaring.Bitmap {
	if vi, ok := val.(*index.BVal); ok {
		tr := q.BucketObj.Btree(key)
		bv := tr.VGet(vi)
		if bv == nil {
			return nil
		}
		return bv.SBMap
	}
	return nil
}
