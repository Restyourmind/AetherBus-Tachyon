package media

import (
	"encoding/binary"
	"fmt"

	"github.com/pierrec/lz4/v4"
)

const lz4HeaderSize = 4

// LZ4Compressor implements the domain.Compressor interface using LZ4.
type LZ4Compressor struct{}

// NewLZ4Compressor creates a new LZ4Compressor.
func NewLZ4Compressor() *LZ4Compressor {
	return &LZ4Compressor{}
}

// Compress compresses the given byte slice and prefixes the original size.
func (c *LZ4Compressor) Compress(data []byte) ([]byte, error) {
	compressedData := make([]byte, lz4HeaderSize+lz4.CompressBlockBound(len(data)))
	binary.BigEndian.PutUint32(compressedData[:lz4HeaderSize], uint32(len(data)))
	n, err := lz4.CompressBlock(data, compressedData[lz4HeaderSize:], nil)
	if err != nil {
		return nil, err
	}
	if n == 0 && len(data) > 0 {
		return nil, fmt.Errorf("failed to compress block")
	}
	return compressedData[:lz4HeaderSize+n], nil
}

// Decompress decompresses the given byte slice using the prefixed original size.
func (c *LZ4Compressor) Decompress(data []byte) ([]byte, error) {
	if len(data) < lz4HeaderSize {
		return nil, fmt.Errorf("failed to decompress with lz4: missing size header")
	}

	decompressedSize := int(binary.BigEndian.Uint32(data[:lz4HeaderSize]))
	if decompressedSize < 0 {
		return nil, fmt.Errorf("failed to decompress with lz4: invalid target size")
	}
	if decompressedSize == 0 {
		return []byte{}, nil
	}

	decompressedData := make([]byte, decompressedSize)
	n, err := lz4.UncompressBlock(data[lz4HeaderSize:], decompressedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress with lz4: %w", err)
	}
	if n != decompressedSize {
		return nil, fmt.Errorf("failed to decompress with lz4: size mismatch got=%d want=%d", n, decompressedSize)
	}

	return decompressedData, nil
}
