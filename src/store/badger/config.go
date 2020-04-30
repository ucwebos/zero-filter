package badger

import "github.com/dgraph-io/badger/v2"

// Config manages Logger and EncryptionKey option for the badger backend
type Config struct {
	EncryptionKey []byte        `json:"-"`
	Logger        badger.Logger `json:"-"`
}
