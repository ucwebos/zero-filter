package entry

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"
	"zero-filter/src/tools"

	"github.com/RoaringBitmap/roaring"
)

type streamQ struct {
	StreamID string
	Bm       *roaring.Bitmap
	Iterator roaring.IntPeekable
	Size     int
	LastTime time.Time
}

func (s *streamQ) XRange() (list []json.RawMessage, end bool) {
	list = make([]json.RawMessage, 0)
	var (
		ukeys = make([][]byte, 0)
	)
	for i := 0; i < s.Size; i++ {
		if !s.Iterator.HasNext() {
			end = true
			streamMap.Delete(s.StreamID)
			break
		}
		x := s.Iterator.Next()
		ukeys = append(ukeys, tools.Int64ToBytes(int64(x)))
	}
	bufs, err := kvReader.MultiGet(ukeys)
	if err != nil {
		return
	}
	for _, buf := range bufs {
		list = append(list, json.RawMessage(buf))
	}
	s.LastTime = time.Now()
	return
}

func getStreamQ(streamID string) (stm *streamQ) {
	if stmi, ok := streamMap.Load(streamID); ok {
		if stm, ok := stmi.(*streamQ); ok {
			return stm
		}
	}
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
		Iterator: bm.Iterator(),
		Size:     1000,
		LastTime: time.Now(),
	}
	streamMap.Store(StreamID, stm)
	return
}
