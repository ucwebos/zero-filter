package store

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
)

// BFile .
type BFile struct {
	Key   string
	KType int
	Val   interface{}
	// File  string
	Fd *os.File
}

func (v *BFile) vToString(val interface{}) string {
	switch x := val.(type) {
	case string:
		return x
	case int:
		s := strconv.Itoa(x)
		return s
	}
	return ""
}

// Exists .
func Exists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

// ValFile .
func (v *BFile) ValFile() string {
	return "./awl/" + v.Key + "_" + md5Str(v.Val) + ".val"
}

func md5Str(val interface{}) string {
	switch x := val.(type) {
	case string:
		m := md5.New()
		m.Write([]byte(x))
		return hex.EncodeToString(m.Sum(nil))
	case int:
		s := strconv.Itoa(x)
		m := md5.New()
		m.Write([]byte(s))
		return hex.EncodeToString(m.Sum(nil))
	}
	return ""
}

// SaveVals .
func (v *BFile) SaveVals(arrv []interface{}) (err error) {
	if v.Fd == nil {
		return errors.New("need do Open! " + "\n")
	}
	for _, rv := range arrv {
		buf := []byte(v.vToString(rv) + "\n")
		v.Fd.Write(buf)
	}
	return
}

// Close .
func (v *BFile) Close() {
	v.Fd.Close()
	v.Fd = nil
}

// Open .
func (v *BFile) Open() (err error) {
	if v.Fd != nil {
		return
	}
	filename := v.ValFile()
	var f *os.File
	if !Exists(filename) {
		f, err = os.Create(filename)
	} else {
		f, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0666)
	}
	if err != nil {
		return
	}
	v.Fd = f
	return
}

// Save .
func (v *BFile) Save(av interface{}) (err error) {
	if v.Fd == nil {
		return errors.New("need do Open! " + "\n")
	}
	buf := []byte(v.vToString(av) + "\n")
	v.Fd.Write(buf)
	return
}

// ValStringMapFromDisk .
func (v *BFile) ValStringMapFromDisk() (vals map[string]bool) {
	filename := v.ValFile()
	vals = make(map[string]bool, 0)
	f, _ := os.Open(filename)
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		rs, err := readLine(r)
		if err != nil {
			break
		}
		vals[rs] = true
	}
	return
}

func readLine(r *bufio.Reader) (string, error) {
	line, isprefix, err := r.ReadLine()
	for isprefix && err == nil {
		var bs []byte
		bs, isprefix, err = r.ReadLine()
		line = append(line, bs...)
	}
	return string(line), err
}
