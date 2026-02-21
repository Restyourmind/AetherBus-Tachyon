package repository

import (
	"sync"

	art "github.com/plar/go-adaptive-radix-tree"
)

// ART_RouteStore is a thread-safe, high-performance routing table using Adaptive Radix Tree.
// It implements the domain.RouteStore interface.
type ART_RouteStore struct {
	tree art.Tree
	mu   sync.RWMutex
}

// NewART_RouteStore creates a new ART_RouteStore.
func NewART_RouteStore() *ART_RouteStore {
	return &ART_RouteStore{
		tree: art.New(),
	}
}

// AddRoute adds a new route to the table.
func (r *ART_RouteStore) AddRoute(topic string, destNodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tree.Insert(art.Key(topic), destNodeID)
}

// Match finds the appropriate destination node ID for a given topic.
// For now, it performs an exact match.
func (r *ART_RouteStore) Match(topic string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	value, found := r.tree.Search(art.Key(topic))
	if found {
		return value.(string)
	}

	// In a real scenario, you might want to implement wildcard matching here.
	// For now, we'll return an empty string if no exact match is found.
	return ""
}
