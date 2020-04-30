package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"zero-filter/src/tools"
)

type item struct {
	ID     string `json:"id"`
	AppID  int    `json:"appId"`
	CateID int    `json:"cateId"`
	RecID  string `json:"recId"`
	UserID int    `json:"userId"`
}

type reqs struct {
	Bucket string `json:"bucket"`
	Data   []item `json:"data"`
}

func main() {
	items := make([]item, 0)
	for i := 1; i <= 1000000; i++ {
		it := item{
			ID:     "xxxxxxxxxxxxx" + strconv.Itoa(i),
			AppID:  rand.Intn(10),
			CateID: rand.Intn(100),
			RecID:  strconv.Itoa(rand.Intn(1000)),
			UserID: rand.Intn(1000),
		}
		items = append(items, it)
		if len(items) >= 10000 {
			put(items)
			items = make([]item, 0)
		}
	}
	put(items)
	items = make([]item, 0)
}

func put(items []item) {
	if len(items) == 0 {
		return
	}
	putURL := "http://localhost:7878/put"

	req := reqs{
		Bucket: "core",
		Data:   items,
	}
	reqBody, _ := tools.JSON.Marshal(req)
	resp, err := tools.PostRaw(nil, putURL, nil, reqBody, 50000)
	fmt.Println(err)
	fmt.Println(string(resp))

}
