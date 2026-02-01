// Package sst:  Overview
//
//	An SST is an immutable, sorted, on-disk file that persists memtable data. When the memtable reaches a size threshold, it's flushed to disk as an SST file.
//	---
//
//	File Format
//
//
//	   1 │+------------------------------------------------------------------+
//	   2 │|                         SST FILE LAYOUT                          |
//	   3 │+------------------------------------------------------------------+
//	   4 │|  DATA BLOCKS                                                     |
//	   5 │|  +-----------------------+                                       |
//	   6 │|  | Data Block 0          |  <- Key-value entries (sorted)        |
//	   7 │|  +-----------------------+                                       |
//	   8 │|  | Data Block 1          |                                       |
//	   9 │|  +-----------------------+                                       |
//	  10 │|  | ...                   |                                       |
//	  11 │|  +-----------------------+                                       |
//	  12 │|  | Data Block N          |                                       |
//	  13 │|  +-----------------------+                                       |
//	  14 │+------------------------------------------------------------------+
//	  15 │|  INDEX BLOCK                                                     |
//	  16 │|  +-----------------------+                                       |
//	  17 │|  | Block 0: first_key -> offset, size                            |
//	  18 │|  | Block 1: first_key -> offset, size                            |
//	  19 │|  | ...                                                           |
//	  20 │|  +-----------------------+                                       |
//	  21 │+------------------------------------------------------------------+
//	  22 │|  BLOOM FILTER (optional)                                         |
//	  23 │|  +-----------------------+                                       |
//	  24 │|  | Bloom filter bits     |  <- Fast "key not present" check      |
//	  25 │|  +-----------------------+                                       |
//	  26 │+------------------------------------------------------------------+
//	  27 │|  FOOTER (fixed 48 bytes)                                         |
//	  28 │|  +-----------------------+                                       |
//	  29 │|  | Index offset     (8)  |                                       |
//	  30 │|  | Index size       (4)  |                                       |
//	  31 │|  | Bloom offset     (8)  |                                       |
//	  32 │|  | Bloom size       (4)  |                                       |
//	  33 │|  | Min key offset   (8)  |                                       |
//	  34 │|  | Min key size     (2)  |                                       |
//	  35 │|  | Max key offset   (8)  |                                       |
//	  36 │|  | Max key size     (2)  |                                       |
//	  37 │|  | CRC32            (4)  |                                       |
//	  38 │|  +-----------------------+                                       |
//	  39 │+------------------------------------------------------------------+
//
//	---
//
//	Data Block Format
//
//	Each data block contains multiple sorted key-value entries:
//
//	   1 │DATA BLOCK (target ~4KB):
//	   2 │+---------------------------------------------------------------+
//	   3 │| Entry 0                                                       |
//	   4 │|   | Key Length (4 bytes) | Value Length (4 bytes) |           |
//	   5 │|   | Key (variable)       | Value (variable)       |           |
//	   6 │+---------------------------------------------------------------+
//	   7 │| Entry 1                                                       |
//	   8 │|   ...                                                         |
//	   9 │+---------------------------------------------------------------+
//	  10 │| Entry N                                                       |
//	  11 │+---------------------------------------------------------------+
//	  12 │| Restart Points (for prefix compression, optional v2)          |
//	  13 │+---------------------------------------------------------------+
//	  14 │| Block CRC32 (4 bytes)                                         |
//	  15 │+---------------------------------------------------------------+
//
//
//	Entry Format (17+ bytes minimum)
//
//
//	   1 │| KEY_LEN (4) | VAL_LEN (4) | TYPE (1) | KEY | VALUE |
//	   2 │
//	   3 │TYPE:
//	   4 │  0x00 = Put (value present)
//	   5 │  0x01 = Delete (tombstone, no value)
//
//	---
//
//	Index Block Format
//
//	Sparse index pointing to data blocks:
//
//	   1 │INDEX BLOCK:
//	   2 │+---------------------------------------------------------------+
//	   3 │| Num Entries (4 bytes)                                         |
//	   4 │+---------------------------------------------------------------+
//	   5 │| Entry 0:                                                      |
//	   6 │|   | Key Length (4) | Key | Block Offset (8) | Block Size (4) ||
//	   7 │+---------------------------------------------------------------+
//	   8 │| Entry 1: ...                                                  |
//	   9 │+---------------------------------------------------------------+
//	  10 │| Index CRC32 (4 bytes)                                         |
//	  11 │+---------------------------------------------------------------+
//
//	---
//
//	Bloom Filter Format (Optional, Phase 2)
//
//
//	   1 │BLOOM FILTER:
//	   2 │+---------------------------------------------------------------+
//	   3 │| Num Hash Functions (4 byte)                                   |
//	   4 │| Bit Array Size (4 bytes)                                      |
//	   5 │| Bit Array (variable)                                          |
//	   6 │| CRC32 (4 bytes)                                               |
//	   7 │+---------------------------------------------------------------+
package sst

type SSTWriter interface {
	Write(
		key []byte,
		value []byte,
	) error
	Flush() error
}

type SSTReader interface {
	ReadFile() (File, error)
}

type File struct {
	dataBlocks []dataBlock
	index      indexBlock
	bloom      bloomFilter
	footer     footer
}

type dataBlock struct {
	crc     uint32
	entries []dataEntry
}

type indexBlock struct {
	numEntries int
	entries    []indexEntry
}

type indexEntry struct {
	key         []byte
	blockOffset int64
	blockSize   uint32
}

type bloomFilter struct {
	m        uint
	k        uint
	bitArray []byte
	crc      uint32
}

type footer struct {
	indexOffset  int64
	indexSize    int
	minKeyOffset int64
	minKeySize   int
	maxKeyOffset int64
	maxKeySize   int
	crc          uint32
}
