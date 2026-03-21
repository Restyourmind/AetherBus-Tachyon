package repository

import (
	"testing"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/aetherbus/aetherbus-tachyon/internal/exporter"
)

type captureExporter struct{ events []exporter.Event }

func (c *captureExporter) Emit(ev exporter.Event) { c.events = append(c.events, ev.Normalize()) }
func (c *captureExporter) Close() error           { return nil }
func (c *captureExporter) Stats() exporter.Stats  { return exporter.Stats{} }

func TestARTStoreEmitsRouteEvents(t *testing.T) {
	store := NewART_RouteStore()
	cap := &captureExporter{}
	store.SetExporter(cap)
	if err := store.UpsertRoute(domain.Route{Pattern: "orders.created", DestinationID: "node-a", RouteType: "direct", Enabled: true, Tenant: "tenant-a"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveRoute(domain.RouteKey{TenantID: "tenant-a", Topic: "orders.created"}, "node-a"); err != nil {
		t.Fatal(err)
	}
	if len(cap.events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(cap.events))
	}
	if cap.events[0].Action != "upsert" || cap.events[1].Action != "remove" {
		t.Fatalf("unexpected actions: %#v", cap.events)
	}
}
