package entry

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/gin-gonic/gin"

	"zero-filter/src/store"
	"zero-filter/src/store/kvstore"
	"zero-filter/src/tools"
)

// 限制流查询数量
const streamSIZE = 1000

var (
	kvReader  kvstore.KVReader
	streamMap sync.Map
)

// Run .
func Run() {
	kvr, err := store.KVReader()
	if err != nil {
		panic(err)
	}
	kvReader = kvr
	// ...
	go func() {
		tick := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-tick.C:
				streamMap.Range(func(k, v interface{}) bool {
					if stm, ok := v.(*streamQ); ok {
						ut := stm.LastTime.Add(1 * time.Minute)
						if ut.Before(time.Now()) {
							streamMap.Delete(k)
						}
					}
					return true
				})
			}
		}
	}()

	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	r.POST("/query", query)
	r.POST("/put", put)
	r.Run(":7878")
}

func query(c *gin.Context) {
	var (
		query = Query{}
		resp  = QueryResp{}
	)
	err := c.BindJSON(&query)
	if err != nil {
		c.JSON(410, gin.H{
			"message": "error query !",
		})
		return
	}
	if query.Bucket == "" {
		c.JSON(410, gin.H{
			"message": "error bucket !",
		})
		return
	}
	if query.Where == nil {
		c.JSON(410, gin.H{
			"message": "error where !",
		})
		return
	}
	// 索引查询
	bm, err := query.Exec()
	if err != nil {
		c.JSON(410, gin.H{
			"message": err.Error(),
		})
		return
	}
	if bm == nil {
		c.JSON(200, resp)
		return
	}
	resp.Size = int(bm.GetCardinality())
	if query.Count {
		c.JSON(200, resp)
		return
	}
	// 分块发送数据流
	if query.Stream {
		var stm *streamQ
		if query.StreamID != "" {
			stm = getStreamQ(query.StreamID)
			if stm == nil {
				c.JSON(410, gin.H{
					"message": "streamID error",
				})
				return
			}
		} else {
			stm, err = createStreamQ(bm)
			if err != nil {
				c.JSON(410, gin.H{
					"message": err.Error(),
				})
			}
		}
		resp.StreamID = stm.StreamID
		resp.List, resp.StreamEnd = stm.XRange()
	} else {
		// 默认limit模式 默认1000个
		var (
			limitStart = 0
			limitSize  = 1000
		)
		// limit裁剪
		if query.Limit != nil {
			limitStart = query.Limit.Start
			limitSize = query.Limit.Size
		}
		// 结果转换
		resp.List = rawLimit(bm, limitStart, limitSize)
	}
	c.JSON(200, resp)
}

func rawLimit(sb *roaring.Bitmap, start int, size int) (list []json.RawMessage) {
	var (
		iNum  = 0
		ukeys = make([][]byte, 0)
	)
	list = make([]json.RawMessage, 0)
	sb.Iterate(func(x uint32) bool {
		if iNum < start {
			iNum++
			return true
		}
		if size == 0 {
			return false
		}
		ukeys = append(ukeys, tools.Int64ToBytes(int64(x)))
		iNum++
		size--
		return true
	})

	bufs, err := kvReader.MultiGet(ukeys)
	if err != nil {
		return
	}
	for _, buf := range bufs {
		list = append(list, json.RawMessage(buf))
	}
	return list
}

func put(c *gin.Context) {
	var (
		req  = PutReq{}
		resp = PutResp{}
	)
	err := c.BindJSON(&req)
	if err != nil {
		c.JSON(410, gin.H{
			"message": "error put !",
		})
		return
	}
	if req.Bucket == "" {
		c.JSON(410, gin.H{
			"message": "error bucket !",
		})
		return
	}
	if req.Data == nil || len(req.Data) == 0 {
		c.JSON(410, gin.H{
			"message": "error data !",
		})
		return
	}
	resp.Ok = req.Put()

	c.JSON(200, resp)
}
