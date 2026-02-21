package domain

// Codec defines the interface for encoding and decoding data.
type Codec interface {
	// Encode encodes the given value into a byte slice.
	Encode(v interface{}) ([]byte, error)
	// Decode decodes the given byte slice into the given value.
	Decode(data []byte, v interface{}) error
}

// Compressor defines the interface for compressing and decompressing data.
type Compressor interface {
	// Compress compresses the given byte slice.
	Compress(data []byte) ([]byte, error)
	// Decompress decompress the given byte slice.
	Decompress(data []byte) ([]byte, error)
}
