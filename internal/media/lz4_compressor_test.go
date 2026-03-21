package media

import (
	"bytes"
	"strings"
	"testing"
)

func TestLZ4CompressorRoundTripLargePayload(t *testing.T) {
	compressor := NewLZ4Compressor()
	payload := []byte(strings.Repeat("aetherbus-tachyon-", 512))

	compressed, err := compressor.Compress(payload)
	if err != nil {
		t.Fatalf("Compress returned error: %v", err)
	}
	if len(compressed) <= lz4HeaderSize {
		t.Fatalf("expected compressed payload with header, got %d bytes", len(compressed))
	}

	decompressed, err := compressor.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress returned error: %v", err)
	}
	if !bytes.Equal(decompressed, payload) {
		t.Fatalf("Decompress payload mismatch")
	}
}

func TestLZ4CompressorRejectsMissingHeader(t *testing.T) {
	compressor := NewLZ4Compressor()
	if _, err := compressor.Decompress([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error for payload without size header")
	}
}
