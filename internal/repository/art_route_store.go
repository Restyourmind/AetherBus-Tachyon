package repository

import (
	"sync"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	art "github.com/plar/go-adaptive-radix-tree"
)

// ART_RouteStore is a thread-safe implementation of the domain.RouteStore
// using a memory-efficient Adaptive Radix Tree.
// It stores a slice of destination node IDs for each topic prefix.
type ART_RouteStore struct {
	tree art.Tree
	mu   sync.RWMutex
}

// NewART_RouteStore creates and initializes a new ART_RouteStore.
func NewART_RouteStore() *ART_RouteStore {
	return &ART_RouteStore{
		tree: art.New(),
	}
}

// AddRoute adds a new route to the store. It is safe for concurrent use.
// It associates a topic (which can be a prefix) with a destination node ID.
func (s *ART_RouteStore) AddRoute(topic string, destNodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := []byte(topic)
	value, found := s.tree.Search(key)

	var destinations []string
	if found {
		destinations, _ = value.([]string)
	}

	// Avoid adding duplicate node IDs for the same topic
	for _, existingID := range destinations {
		if existingID == destNodeID {
			return
		}
	}

	destinations = append(destinations, destNodeID)
	s.tree.Insert(key, destinations)
}

// Match finds the longest prefix match for a given topic and returns
// one of the associated destination node IDs. It is safe for concurrent use.
func (s *ART_RouteStore) Match(topic string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := []byte(topic)
	_, value, found := s.tree.LongestPrefix(key)

	if found {
		destinations, ok := value.([]string)
		if ok && len(destinations) > 0 {
			// In a real-world scenario, you might have a load balancing
			// strategy here (e.g., random, round-robin).
			// For simplicity, we return the first one.
			return destinations[0]
		}
	}

	return ""
}
