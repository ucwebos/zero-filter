package main

import (
	"fmt"
	"time"
	"zero-filter/src/entry"
	"zero-filter/src/tools"
)

func main() {
	queryURL := "http://localhost:7878/query"

	wheres := []entry.WhereOp{
		{
			Key: "recId",
			Op:  ">=",
			Val: 0,
		},
	}
	req := entry.Query{
		Bucket: "core",
		Where:  wheres,
		Stream: true,
	}

	resp := entry.QueryResp{}
	st := time.Now().Unix()
	num := 0
	for {

		reqBody, _ := tools.JSON.Marshal(req)
		err := tools.PostWithUnmarshal(nil, queryURL, nil, reqBody, &resp, 50000)
		if err != nil {
			fmt.Println(err)
		}
		if resp.StreamEnd == true || resp.StreamID == "" {
			break
		}
		num = num + len(resp.List)
		if num%100000 == 0 {
			t := time.Now().Unix()
			fmt.Println(t - st)
			fmt.Println(num)
			fmt.Println(resp.StreamID)

		}
		req.StreamID = resp.StreamID
	}
	t := time.Now().Unix()
	fmt.Println(t - st)
	fmt.Println(num)
}
