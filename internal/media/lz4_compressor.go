package media

import (
	"github.com/pierrec/lz4/v4"
)

// LZ4Compressor implements the domain.Compressor interface using LZ4.
type LZ4Compressor struct{}

// NewLZ4Compressor creates a new LZ4Compressor.
func NewLZ4Compressor() *LZ4Compressor {
	return &LZ4Compressor{}
}

// Compress compresses the given byte slice.
func (c *LZ4Compressor) Compress(data []byte) ([]byte, error) {
	compressedData := make([]byte, lz4.CompressBlockBound(len(data)))
	n, err := lz4.CompressBlock(data, compressedData, nil)
	if err != nil {
		return nil, err
	}
	return compressedData[:n], nil
}

// Decompress decompress the given byte slice.
func (c *LZ4Compressor) Decompress(data []byte) ([]byte, error) {
	// The decompressed size must be known. In a real-world scenario,
	// this would be sent as part of the message frame.
	// For now, we'll assume a reasonable max size.
	decompressedData := make([]byte, 10*len(data)) // Placeholder size
	n, err := lz4.UncompressBlock(data, decompressedData)
	if err != nil {
		return nil, err
	}
	return decompressedData[:n], nil
}
