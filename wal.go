package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	InvalidCRC   = uint32(0xFFFFFFFF)
	MaxEntrySize = 16 << 20 // 16MB
)

var ErrCorruptWAL = fmt.Errorf("corrupt WAL")

type Operation int

const (
	OperationPut Operation = iota
	OperationDelete
)

type Log struct {
	op    Operation
	key   []byte
	value []byte
	crc   uint32

	done chan error
}

func (l *Log) String() string {
	return fmt.Sprintf("[crc: ] [operation: %d] [key: %s] [value: %s]", l.op, l.key, l.value)
}

// Encode Binary format:
// | CRC (4) | TOTAL_LEN (4) | TYPE (1) | KEY_LEN (4) | KEY | VAL_LEN (4) | VALUE |
// CRC = checksum(TOTAL_LEN | PAYLOAD)
func (l *Log) Encode(w io.Writer) error {
	seeker, ok := w.(io.Seeker)
	if !ok {
		return fmt.Errorf("wal writer must be seekable")
	}

	crc := crc32.NewIEEE()
	mw := io.MultiWriter(w, crc)

	keyLen := uint32(len(l.key))
	valLen := uint32(len(l.value))

	payloadLen := 1 + 4 + keyLen + 4 + valLen
	totalLen := 4 + payloadLen

	if totalLen > MaxEntrySize {
		return fmt.Errorf("entry too large")
	}

	if err := binary.Write(w, binary.LittleEndian, InvalidCRC); err != nil {
		return err
	} // update later

	// total len
	if err := binary.Write(mw, binary.LittleEndian, totalLen); err != nil {
		return err
	}

	// TYPE
	if err := binary.Write(mw, binary.LittleEndian, byte(l.op)); err != nil {
		return err
	}

	// KEY_LEN
	if err := binary.Write(mw, binary.LittleEndian, keyLen); err != nil {
		return err
	}

	// KEY
	if _, err := mw.Write(l.key); err != nil {
		return err
	}

	// VAL_LEN
	if err := binary.Write(mw, binary.LittleEndian, valLen); err != nil {
		return err
	}

	// VALUE
	if _, err := mw.Write(l.value); err != nil {
		return err
	}

	pos, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	_, err = seeker.Seek(pos-int64(totalLen)-4, io.SeekStart)
	if err != nil {
		return err
	}

	if err = binary.Write(w, binary.LittleEndian, crc.Sum32()); err != nil {
		return err
	}

	if _, err = seeker.Seek(pos, io.SeekStart); err != nil {
		return err
	}

	return nil
}

func cleanEOF(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return io.EOF
	}
	return err
}

func Decode(r io.Reader) (*Log, error) {
	var storedCRC uint32
	if err := binary.Read(r, binary.LittleEndian, &storedCRC); err != nil {
		return nil, cleanEOF(err)
	}

	if storedCRC == InvalidCRC {
		return nil, io.EOF
	}

	var totalLen uint32
	if err := binary.Read(r, binary.LittleEndian, &totalLen); err != nil {
		return nil, cleanEOF(err)
	}

	if totalLen > MaxEntrySize || totalLen < 5 {
		return nil, ErrCorruptWAL
	}

	payload := make([]byte, totalLen)
	binary.LittleEndian.PutUint32(payload[0:4], totalLen)

	if _, err := io.ReadFull(r, payload[4:]); err != nil {
		return nil, cleanEOF(err)
	}

	if crc32.ChecksumIEEE(payload) != storedCRC {
		return nil, ErrCorruptWAL
	}

	pos := 4

	var l Log
	l.crc = storedCRC

	l.op = Operation(payload[pos])
	pos++

	keyLen := binary.LittleEndian.Uint32(payload[pos:])
	pos += 4

	if keyLen > uint32(len(payload))-uint32(pos) {
		return nil, ErrCorruptWAL
	}

	l.key = make([]byte, keyLen)
	copy(l.key, payload[pos:pos+int(keyLen)])
	pos += int(keyLen)

	valLen := binary.LittleEndian.Uint32(payload[pos:])
	pos += 4

	if valLen > uint32(len(payload))-uint32(pos) {
		return nil, ErrCorruptWAL
	}

	l.value = make([]byte, valLen)
	copy(l.value, payload[pos:pos+int(valLen)])

	return &l, nil
}
