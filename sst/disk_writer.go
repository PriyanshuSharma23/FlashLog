package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/Priyanshu23/FlashLogGo/types"
	"github.com/bits-and-blooms/bloom/v3"
)

const (
	filename                = "segment-001.sst"
	defaultMaxDataBlockSize = 4 * 1024 // 4kB
)

type diskSSTWriter struct {
	dir               string
	currDataBlockSize int
	maxDataBlockSize  int
	currDataBlock     dataBlock
	sstFile           *os.File
	index             indexBlock
	minKey            []byte
	maxKey            []byte
	bloomFilter       *bloom.BloomFilter
}

type dataEntry struct {
	key   []byte
	value []byte
}

func (d *dataEntry) size() int {
	return 4 + 4 + 1 + len(d.key) + len(d.value)
}

func NewDiskSSTWriter(dir string) (SSTWriter, error) {
	file, err := os.Create(filepath.Join(dir, filename))
	if err != nil {
		return nil, fmt.Errorf("failed to create SST file: %w", err)
	}

	filter := bloom.NewWithEstimates(100000, 0.01)

	return &diskSSTWriter{
		dir:               dir,
		currDataBlockSize: 0,
		maxDataBlockSize:  defaultMaxDataBlockSize,
		sstFile:           file,
		bloomFilter:       filter,
	}, nil
}

func (d *diskSSTWriter) recordIndex(blockOffset int64, blockSize uint32) {
	if len(d.currDataBlock.entries) == 0 {
		return
	}

	firstKey := d.currDataBlock.entries[0].key

	keyCopy := make([]byte, len(firstKey))
	copy(keyCopy, firstKey)

	d.index.entries = append(d.index.entries, indexEntry{
		key:         keyCopy,
		blockOffset: blockOffset,
		blockSize:   blockSize,
	})
}

func (d *diskSSTWriter) appendDataBlock() error {
	blockStart, _ := d.sstFile.Seek(0, io.SeekCurrent)

	_ = binary.Write(d.sstFile, binary.LittleEndian, uint32(0))

	crc := crc32.NewIEEE()
	mw := io.MultiWriter(d.sstFile, crc)

	for _, e := range d.currDataBlock.entries {
		_ = binary.Write(mw, binary.LittleEndian, uint32(len(e.key)))
		_ = binary.Write(mw, binary.LittleEndian, uint32(len(e.value)))
		_ = binary.Write(mw, binary.LittleEndian, uint8(e.op))
		_, _ = mw.Write(e.key)
		_, _ = mw.Write(e.value)
	}

	// compute actual payload size
	payloadEnd, _ := d.sstFile.Seek(0, io.SeekCurrent)
	payloadSize := uint32(payloadEnd - blockStart - 4)

	// write crc
	_ = binary.Write(d.sstFile, binary.LittleEndian, crc.Sum32())

	// patch block size
	finalEnd, _ := d.sstFile.Seek(0, io.SeekCurrent)
	_, _ = d.sstFile.Seek(blockStart, io.SeekStart)
	_ = binary.Write(d.sstFile, binary.LittleEndian, payloadSize)
	_, _ = d.sstFile.Seek(finalEnd, io.SeekStart)

	// index needs this
	d.recordIndex(blockStart, payloadSize+4)

	return nil
}

func (d *diskSSTWriter) Write(
	key []byte,
	value []byte,
) error {
	if d.minKey == nil || bytes.Compare(key, d.minKey) < 0 {
		d.minKey = append([]byte(nil), key...)
	}
	if d.maxKey == nil || bytes.Compare(key, d.maxKey) > 0 {
		d.maxKey = append([]byte(nil), key...)
	}

	entry := dataEntry{
		key:   key,
		value: value,
	}

	if entry.size()+d.currDataBlockSize > d.maxDataBlockSize {
		err := d.appendDataBlock()
		if err != nil {
			return err
		}

		d.currDataBlock = dataBlock{
			crc:     0,
			entries: []dataEntry{},
		}
		d.currDataBlockSize = 0
	}

	d.currDataBlock.entries = append(d.currDataBlock.entries, entry)
	d.currDataBlockSize += entry.size()

	d.bloomFilter.Add(key)

	return nil
}

func (d *diskSSTWriter) writeIndexBlock() (int64, uint32, error) {
	start, _ := d.sstFile.Seek(0, io.SeekCurrent)

	crc := crc32.NewIEEE()
	mw := io.MultiWriter(d.sstFile, crc)

	_ = binary.Write(mw, binary.LittleEndian, uint32(len(d.index.entries)))

	for _, e := range d.index.entries {
		_ = binary.Write(mw, binary.LittleEndian, uint32(len(e.key)))
		_, _ = mw.Write(e.key)
		_ = binary.Write(mw, binary.LittleEndian, e.blockOffset)
		_ = binary.Write(mw, binary.LittleEndian, e.blockSize)
	}

	_ = binary.Write(d.sstFile, binary.LittleEndian, crc.Sum32())

	end, _ := d.sstFile.Seek(0, io.SeekCurrent)
	return start, uint32(end - start), nil
}

func (d *diskSSTWriter) writeBloomFilter() (int64, uint32, error) {
	start, err := d.sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to seek start of file: %w", err)
	}

	crc := crc32.NewIEEE()
	mw := io.MultiWriter(d.sstFile, crc)

	err = binary.Write(mw, binary.LittleEndian, uint32(d.bloomFilter.K()))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write bloom filter hash count: %w", err)
	}

	err = binary.Write(mw, binary.LittleEndian, uint32(d.bloomFilter.Cap()))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write bloom filter size: %w", err)
	}

	_, err = d.bloomFilter.WriteTo(mw)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write bloom filter bit array: %w", err)
	}

	err = binary.Write(d.sstFile, binary.LittleEndian, crc.Sum32())
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write bloom filter crc: %w", err)
	}

	end, err := d.sstFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to seek end of file: %w", err)
	}

	return start, uint32(end - start), nil
}

func (d *diskSSTWriter) writeFooter(indexOffset int64, indexSize uint32, bloomFilterOffset int64, bloomFilterSize uint32) error {
	footerStart, _ := d.sstFile.Seek(0, io.SeekCurrent)

	crc := crc32.NewIEEE()
	mw := io.MultiWriter(d.sstFile, crc)

	// Index location
	_ = binary.Write(mw, binary.LittleEndian, indexOffset)
	_ = binary.Write(mw, binary.LittleEndian, indexSize)

	err := binary.Write(mw, binary.LittleEndian, bloomFilterOffset)
	if err != nil {
		return fmt.Errorf("failed to write bloom filter offset: %w", err)
	}

	err = binary.Write(mw, binary.LittleEndian, bloomFilterSize)
	if err != nil {
		return fmt.Errorf("failed to write bloom filter size: %w", err)
	}

	// Min key
	minKeyOffset := footerStart +
		8 + 4 + // index
		8 + 4 + // bloom
		8 + 2 + // min key offset + len
		8 + 2 // max key offset + len

	_ = binary.Write(mw, binary.LittleEndian, minKeyOffset)
	_ = binary.Write(mw, binary.LittleEndian, uint16(len(d.minKey)))

	// Max key
	maxKeyOffset := minKeyOffset + int64(len(d.minKey))
	_ = binary.Write(mw, binary.LittleEndian, maxKeyOffset)
	_ = binary.Write(mw, binary.LittleEndian, uint16(len(d.maxKey)))

	// Write keys themselves
	_, _ = mw.Write(d.minKey)
	_, _ = mw.Write(d.maxKey)

	// CRC
	_ = binary.Write(d.sstFile, binary.LittleEndian, crc.Sum32())

	return nil
}

func (d *diskSSTWriter) Flush() error {
	if len(d.currDataBlock.entries) > 0 {
		if err := d.appendDataBlock(); err != nil {
			return err
		}
	}

	indexOffset, indexSize, err := d.writeIndexBlock()
	if err != nil {
		return err
	}

	bloomFilterOffset, bloomFilterSize, err := d.writeBloomFilter()
	if err != nil {
		return err
	}

	return d.writeFooter(indexOffset, indexSize, bloomFilterOffset, bloomFilterSize)
}
