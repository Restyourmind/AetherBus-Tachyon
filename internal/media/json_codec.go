package media

import "encoding/json"

// JSONCodec implements the domain.Codec interface using JSON.
type JSONCodec struct{}

// NewJSONCodec creates a new JSONCodec.
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

// Encode encodes the given value into a byte slice.
func (c *JSONCodec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Decode decodes the given byte slice into the given value.
func (c *JSONCodec) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
