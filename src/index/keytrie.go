package index

// KeyTrie .
var KeyTrie = &IKeyTrie{
	nodes: make(map[rune]*IKeyTrie),
	val:   0,
}

// IKeyTrie .
type IKeyTrie struct {
	nodes map[rune]*IKeyTrie
	val   uint32
}

// Put .
func (ir *IKeyTrie) Put(key string, uKey32 uint32) {
	for _, rn := range key {
		if _, ok := ir.nodes[rn]; !ok {
			node := &IKeyTrie{
				nodes: make(map[rune]*IKeyTrie),
				val:   0,
			}
			ir.nodes[rn] = node
		}
		ir = ir.nodes[rn]
	}
	ir.val = uKey32
}

// Get .
func (ir *IKeyTrie) Get(key string) (uKey32 uint32) {
	for _, rn := range key {
		node, ok := ir.nodes[rn]
		if !ok {
			return
		}
		ir = node
	}
	return ir.val
}
