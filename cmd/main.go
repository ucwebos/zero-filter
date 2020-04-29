package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"zero-filter/src/config"
	"zero-filter/src/entry"
	"zero-filter/src/index"
	"zero-filter/src/store"
	"zero-filter/src/writer"
)

func init() {
	flag.StringVar(&config.Cf, "c", "/etc/zf.conf.json", "配置文件路径")
	flag.Parse()
	config.Init()
}

func main() {
	go func() {
		http.ListenAndServe("0.0.0.0:6060", nil)
	}()

	defer func() {
		if err := recover(); err != nil {
			// 异常时落地数据
			for _, buk := range index.Buckets {
				buk.Sync()
			}
			fmt.Printf("%s\n", err)
		}
	}()
	index.Init()
	store.Init()
	writer.Init()
	entry.Run()
}
