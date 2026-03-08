package fastpath

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

const (
	magicBytes                 = 0xAE7B
	version                    = 1
	SidecarLargePayloadCutover = 256 * 1024

	OpEncodeFrame = 1
	OpPeekHeader  = 2
	OpCompress    = 3
	OpDecompress  = 4
)

type Client struct {
	mu   sync.Mutex
	conn *net.UnixConn
}

func Dial(path string) (*Client, error) {
	addr := &net.UnixAddr{Name: path, Net: "unix"}
	conn, err := net.DialUnix("unix", nil, addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) EncodeFrame(flags byte, topic string, payload []byte) ([]byte, error) {
	if len(payload) <= SidecarLargePayloadCutover {
		return encodeFrameLocal(flags, topic, payload), nil
	}

	return c.encodeFrameSidecar(flags, topic, payload)
}

func encodeFrameLocal(flags byte, topic string, payload []byte) []byte {
	frame := make([]byte, 10+len(topic)+len(payload))
	binary.BigEndian.PutUint16(frame[0:2], magicBytes)
	frame[2] = version
	frame[3] = flags
	binary.BigEndian.PutUint16(frame[4:6], uint16(len(topic)))
	binary.BigEndian.PutUint32(frame[6:10], uint32(len(payload)))
	copy(frame[10:10+len(topic)], []byte(topic))
	copy(frame[10+len(topic):], payload)
	return frame
}

func (c *Client) encodeFrameSidecar(flags byte, topic string, payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil, fmt.Errorf("fastpath client is closed (payload=%d > %d requires sidecar)", len(payload), SidecarLargePayloadCutover)
	}

	req := make([]byte, 8+len(topic)+len(payload))
	req[0] = OpEncodeFrame
	req[1] = flags
	binary.BigEndian.PutUint16(req[2:4], uint16(len(topic)))
	binary.BigEndian.PutUint32(req[4:8], uint32(len(payload)))
	copy(req[8:8+len(topic)], []byte(topic))
	copy(req[8+len(topic):], payload)

	if _, err := c.conn.Write(req); err != nil {
		return nil, err
	}

	respHeader := make([]byte, 6)
	if _, err := io.ReadFull(c.conn, respHeader); err != nil {
		return nil, err
	}

	status := binary.BigEndian.Uint16(respHeader[0:2])
	bodyLen := binary.BigEndian.Uint32(respHeader[2:6])
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		return nil, err
	}

	if status != 0 {
		return nil, fmt.Errorf("sidecar error status=%d body=%q", status, string(body))
	}
	return body, nil
}
