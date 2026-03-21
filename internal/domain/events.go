package domain

import "time"

// Event represents a single, immutable event that has occurred in the system.
// It's the primary data structure that flows through AetherBus-Tachyon.
type Event struct {
	// ID is a unique identifier for the event (e.g., UUID).
	ID string

	// Topic is the subject of the event (e.g., "user.created").
	// It's used for routing.
	Topic string

	// Source is the identifier of the client that published the event.
	Source string

	// Timestamp is the time the event was created, in UTC.
	Timestamp time.Time `json:"timestamp"`

	// DeliverAt requests that broker dispatch not start before this UTC timestamp.
	DeliverAt time.Time `json:"deliver_at,omitempty"`

	// Data is the payload of the event. It can be any structured data.
	Data interface{}

	// DataContentType specifies the MIME type of the data (e.g., "application/json").
	DataContentType string

	// DataSchema is a URI to the schema of the data.
	DataSchema string

	// SpecVersion is the version of the CloudEvents specification.
	SpecVersion string
}

// Envelope is a container for an event that includes routing information.
// This is the structure that is passed to the use case layer.
type Envelope struct {
	// ClientID is the ID of the client that sent the event.
	ClientID []byte
	// Event is the event itself.
	Event Event
	// DeliverAt requests that broker dispatch does not begin before this UTC timestamp.
	DeliverAt time.Time
	// NextAttemptAt is broker-managed scheduler state for delayed retries/promotions.
	NextAttemptAt time.Time
}
