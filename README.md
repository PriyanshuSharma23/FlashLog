# Flashlog

A high-performance Write-Ahead Log (WAL) implementation in Go, designed for durability and crash recovery in storage engines.

## Features

- **CRC32 Checksums** — Every log entry is protected with CRC32 checksums to detect corruption
- **Binary Encoding** — Efficient, compact binary format for minimal storage overhead
- **Segment Rotation** — Automatic log segmentation with configurable size limits (default 16MB)
- **Thread-Safe** — Concurrent access with proper synchronization
- **Crash Recovery** — Designed to recover from incomplete writes and detect corrupted entries

## Usage

### WAL Entry Format

Each log entry follows this binary format:

```
| CRC (4) | TOTAL_LEN (4) | TYPE (1) | KEY_LEN (4) | KEY | VAL_LEN (4) | VALUE |
```

- **CRC**: CRC32 checksum of the payload (4 bytes)
- **TOTAL_LEN**: Total length of the entry excluding CRC (4 bytes)
- **TYPE**: Operation type - Put (0) or Delete (1) (1 byte)
- **KEY_LEN**: Length of the key (4 bytes)
- **KEY**: Variable-length key data
- **VAL_LEN**: Length of the value (4 bytes)
- **VALUE**: Variable-length value data

### Segment Manager

The segment manager handles automatic log rotation:

```go
import "github.com/Priyanshu23/FlashLogGo/segmentmanager"

// Create a new disk segment manager
sm, err := segmentmanager.NewDiskSegmentManager("/path/to/wal",
    segmentmanager.WithMaxSegmentSize(32 * 1024 * 1024), // 32MB segments
)
if err != nil {
    log.Fatal(err)
}
defer sm.Close()

// Write to the active segment
err = sm.WriteActive(entrySize, func(w io.Writer) {
    // Write your data here
})
```

### Configuration Options

| Option               | Default | Description                                      |
| -------------------- | ------- | ------------------------------------------------ |
| `WithMaxSegmentSize` | 16MB    | Maximum size of each log segment before rotation |

## Design

Flashlog is inspired by the Write-Ahead Logging techniques described in _Designing Data-Intensive Applications_ by Martin Kleppmann. Key design decisions:

1. **Append-Only Writes**: All writes are sequential appends, optimizing for disk I/O
2. **Atomic Entries**: Each entry is self-contained with its own checksum
3. **Lazy CRC Computation**: CRC is computed incrementally during encoding
4. **Segment Files**: Logs are split into numbered segment files (`segment-0001.log`, `segment-0002.log`, etc.)

## Development

### Prerequisites

- Go 1.21 or later

### Running Tests

```bash
go test ./...
```

### Project Structure

```
.
├── main.go                 # DB interface and command types
├── wal.go                  # WAL entry encoding/decoding
├── wal_test.go            # WAL tests
├── wal_writer_test.go     # WAL writer tests
└── segmentmanager/
    ├── segmentmanager.go  # Segment manager interface
    ├── disk.go            # Disk-based segment implementation
    └── disk_test.go       # Segment manager tests
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
