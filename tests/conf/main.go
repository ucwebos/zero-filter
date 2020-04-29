package main

import (
	"fmt"
	"zero-filter/src/config"
	"zero-filter/src/tools"
)

func main() {
	buf, _ := tools.JSON.Marshal(config.GConfig)
	fmt.Println(string(buf))
}
