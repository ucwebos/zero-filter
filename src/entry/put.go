package entry

import "zero-filter/src/writer"

// PutReq .
type PutReq struct {
	Bucket string                   `json:"bucket"` //bucket
	Data   []map[string]interface{} `json:"data"`   //data
}

// PutResp .
type PutResp struct {
	Ok bool `json:"ok"` //ok
}

// Put .
func (p *PutReq) Put() (ok bool) {
	wr := writer.GetBucketWr(p.Bucket)
	wr.Put(p.Data)
	return true
}
