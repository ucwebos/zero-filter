package entry

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/RoaringBitmap/roaring"
)

type streamQ struct {
	StreamID string
	Bm       *roaring.Bitmap
	Count    int
	Size     int
	Offset   int
	LastTime time.Time
}

func (s *streamQ) XRange() (list []json.RawMessage) {
	list = rawLimit(s.Bm, s.Offset, s.Size)
	s.Offset = s.Offset + s.Size
	if s.Offset >= s.Count {
		streamMap.Delete(s.StreamID)
	}
	s.LastTime = time.Now()
	return
}

func createStreamQ(bm *roaring.Bitmap) (stm *streamQ, err error) {
	count := 0
	streamMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	if count >= streamSIZE {
		err = errors.New("Stream Query Full	Please Try It Later ")
		return
	}
	StreamID := strconv.Itoa(count) + "_" + strconv.Itoa(int(time.Now().UnixNano()))
	stm = &streamQ{
		StreamID: StreamID,
		Bm:       bm,
		Count:    int(bm.GetCardinality()),
		Size:     1000,
		Offset:   0,
		LastTime: time.Now(),
	}
	streamMap.Store(StreamID, stm)
	return
}
