package domain

// Status represents the state of a node in the cluster.
type Status uint8

const (
	Alive Status = iota
	Suspect
	Dead
)

// Event represents the core data packet in AetherBus.
type Event struct {
	ID        string // UUID format
	Topic     string
	Payload   []byte
	Timestamp int64  // Unix nanoseconds
}

// Node represents a member in the AetherBus cluster.
type Node struct {
	ID          string
	Address     string
	Incarnation uint32
	Status      Status
}
