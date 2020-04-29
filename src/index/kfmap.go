package index

import (
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"

	"zero-filter/src/config"
	"zero-filter/src/pb"
	"zero-filter/src/tools"
)

// KFMap .
type KFMap struct {
	Partition int
	Dir       string
}

// NewKFMap .
func NewKFMap(name string, partition int) (kfm *KFMap) {
	var (
		dirPath = config.GConfig.Path + "/" + name + "_KFMAP"
	)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return
	}
	return &KFMap{
		Partition: partition,
		Dir:       dirPath,
	}
}

// BatchSet .
func (km *KFMap) BatchSet(kvmap map[string]uint32) {
	groups := make(map[uint32]map[string]uint32)
	for key, uKey32 := range kvmap {
		var (
			itmp = crc32.ChecksumIEEE([]byte(key))
			p    = itmp % uint32(km.Partition)
			pk   = p + 1
		)
		if _, ok := groups[pk]; !ok {
			groups[pk] = map[string]uint32{}
		}
		groups[pk][key] = uKey32
	}
	for pk, itmap := range groups {
		km.save(pk, itmap)
	}
}

func (km *KFMap) save(pk uint32, itmap map[string]uint32) (err error) {
	var (
		pfname  = km.Dir + "/" + strconv.Itoa(int(pk))
		content []byte
		pbKvMap = &pb.PKMap{
			Map: make(map[string]int32),
		}
	)
	if tools.Exists(pfname) {
		content, err = ioutil.ReadFile(pfname)
		if err != nil {
			return
		}
	}
	if content != nil {
		bufo, err := snappy.Decode(nil, content)
		if err == nil {
			proto.Unmarshal(bufo, pbKvMap)
		}
	}
	for key, uKey32 := range itmap {
		pbKvMap.Map[key] = int32(uKey32)
	}
	buf, _ := proto.Marshal(pbKvMap)
	bufCp := snappy.Encode(nil, buf)
	return ioutil.WriteFile(pfname, bufCp, os.ModePerm)
}

func (km *KFMap) read(pk uint32) (pbKvMap *pb.PKMap, err error) {
	var (
		pfname  = km.Dir + "/" + strconv.Itoa(int(pk))
		content []byte
	)
	pbKvMap = &pb.PKMap{
		Map: make(map[string]int32),
	}
	if tools.Exists(pfname) {
		content, err = ioutil.ReadFile(pfname)
		if err != nil {
			return
		}
		bufo, err2 := snappy.Decode(nil, content)
		if err2 != nil {
			err = err2
			return
		}
		proto.Unmarshal(bufo, pbKvMap)
	}
	return
}

// BatchGet .
func (km *KFMap) BatchGet(keys []string) (uKeyMap map[string]uint32) {
	var (
		groups = make(map[uint32][]string)
	)
	uKeyMap = make(map[string]uint32)
	for _, key := range keys {
		var (
			itmp = crc32.ChecksumIEEE([]byte(key))
			p    = itmp % uint32(km.Partition)
			pk   = p + 1
		)
		if _, ok := groups[pk]; !ok {
			groups[pk] = make([]string, 0)
		}
		groups[pk] = append(groups[pk], key)
	}
	for pk, itkeys := range groups {
		kvmap, err := km.read(pk)
		if err != nil {
			fmt.Println(err)
			// log ...
			continue
		}
		for _, key := range itkeys {
			if v, ok := kvmap.Map[key]; ok {
				uKeyMap[key] = uint32(v)
			}
		}
	}
	return
}
