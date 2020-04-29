package search

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cstockton/go-conv"
	"github.com/expectedsh/go-sonic/sonic"
)

// Search .
type Search struct {
	Bucket   string
	Ingester sonic.Ingestable
	Searcher sonic.Searchable
}

// NewSearch .
func NewSearch(bucket string) *Search {
	var (
		host     = "localhost"
		port     = 1491
		password = "SecretPassword"
	)
	ingester, err := sonic.NewIngester(host, port, password)
	if err != nil {
		panic(err)
	}
	search, err := sonic.NewSearch(host, port, password)
	if err != nil {
		panic(err)
	}
	return &Search{
		Bucket:   bucket,
		Ingester: ingester,
		Searcher: search,
	}
}

// Put .
func (s *Search) Put(key string, id uint32, otxt string) (ok bool) {
	idStr, err := conv.String(id)
	if err != nil {
		return
	}
	err = s.Ingester.Push(s.Bucket, key, idStr, otxt)
	if err != nil {
		return
	}
	return true
}

// Flush  .
func (s *Search) Flush(key string) (ok bool) {
	err := s.Ingester.FlushBucket(s.Bucket, key)
	if err != nil {
		return
	}
	return true
}

// Search .
func (s *Search) Search(key string, obj string) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	var (
		offset = 0
		size   = 1000
	)
	for {
		results, err := s.Searcher.Query(s.Bucket, key, obj, size, offset)
		if err != nil || len(results) == 0 {
			return
		}
		for _, id := range results {
			idU32, err := conv.Uint32(id)
			if err != nil {
				continue
			}
			bm.Add(idU32)
		}
		offset = offset + size
	}
}
