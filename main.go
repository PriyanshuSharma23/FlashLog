package main

type DB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Close() error
}

type Command int

const (
	CommandUnknown Command = iota
	CommandInsert
	CommandUpdate
	CommandDelete
)

func main() {
}
