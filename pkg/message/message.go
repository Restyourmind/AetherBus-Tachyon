//go:build legacy
// +build legacy

package message

import (
	"sync"
	"time"

	pb "github.com/aetherbus/aetherbus/internal/proto"
)

// Message is the basic unit of communication.
// It is used by the client and the broker.
//
// A message can be a request, a reply or a broadcast.
//
// A request message has an ID, a destination, a timeout and a body.
// It can also have a reply channel, if the sender expects a reply.
//
// A reply message has an ID, a destination (the original sender), a timeout and a body.
// The ID must be the same as the request ID.
// It can also have an error, if the request failed.
//
// A broadcast message has a destination (a topic), a timeout and a body.
// It has no ID and no reply channel.
//
// The destination is a string that identifies the target of the message.
// It can be a service name, a topic name or a client ID.
//
// The timeout is the time the sender is willing to wait for a reply.
// It is used by the client to cancel a request.
// It is also used by the broker to expire a message.
//
// The body is the actual content of the message.
// It is a byte array, that can be encoded in any format.
// The default format is JSON.
//
// The message is managed by a sync.Pool to reduce garbage collection.
// The pool is created by the NewPool function.
// The Get and Put functions are used to get and return messages from the pool.
//
// The message has a reference counter to keep track of the number of users.
// The Incr and Decr functions are used to increment and decrement the counter.
// When the counter reaches zero, the message is returned to the pool.
//
// The message has a number of helper methods to get and set the message properties.
// These methods are used by the client and the broker.
// For example, the IsRequest, IsReply and IsBroadcast methods are used to check the message type.
// The IsExpired method is used to check if the message has expired.
//
// The message also has a number of methods to manipulate the message body.
// The pack and unpack methods are used to encode and decode the message body.
// The SetBody and GetBody methods are used to set and get the message body.
// These methods use a codec to encode and decode the message body.
// The default codec is JSON.

type (
	Message    = pb.Message
	MessageRef = *pb.Message
	Envelope   = pb.Envelope
)

var pool = sync.Pool{
	New: func() interface{} {
		return new(Message)
	},
}

func New() MessageRef {
	m := pool.Get().(MessageRef)
	m.Reset()
	return m
}

func Free(m MessageRef) {
	pool.Put(m)
}

func (m *Message) IsExpired() bool {
	if m.ExpiresAt == 0 {
		return false
	}
	return time.Now().UnixNano() > m.ExpiresAt
}

func (m *Message) IsRequest() bool {
	return m.Id != "" && m.ReplyTo != ""
}

func (m *Message) IsReply() bool {
	return m.Id != "" && m.ReplyTo == ""
}

func (m *Message) IsBroadcast() bool {
	return m.Id == ""
}
