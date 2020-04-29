package config

import (
	"fmt"
	"io/ioutil"

	"zero-filter/src/tools"
)

const (
	// KTypeINT .
	KTypeINT = 1
	// KTypeString .
	KTypeString = 2
)

// Idx .
type Idx struct {
	KType int `json:"kType",yaml:"kType"` //1.int 2.string
}

// Bucket .
type Bucket struct {
	ModeUNI  bool            `json:"modeUNI",yaml:"modeUNI"`
	Primary  string          `json:"primary",yaml:"primary"`
	KFMapNum int             `json:"kfMapNum",yaml:"kfMapNum"`
	Idxs     map[string]Idx  `json:"idxs",yaml:"idxs"`
	Search   map[string]bool `json:"search",yaml:"search"`
}

// Config .
type Config struct {
	Port    string            `json:"port",yaml:"port"`
	Path    string            `json:"path",yaml:"path"`
	Buckets map[string]Bucket `json:"buckets",yaml:"buckets"`
}

// GConfig 全局配置
var (
	Cf      = ""
	GConfig = &Config{}
)

// Init .
func Init() {
	if !tools.Exists(Cf) {
		panic("配置文件不存在 ")
	}
	buf, err := ioutil.ReadFile(Cf)
	if err != nil {
		fmt.Println(err)
		panic("配置文件读取失败 ")
	}
	err = tools.JSON.Unmarshal(buf, &GConfig)
	if err != nil {
		fmt.Println(err)
		panic("配置文件解析失败 ")
	}
	//todo 判断 path 。。。

}
