package distributed

import "zero-filter/src/index"

// Shard .
type Shard struct {
	IP      string
	Primary *index.Bucket
	Replica *index.Bucket
}
