package state

import (
	"github.com/aetherbus/aetherbus-tachyon/internal/admin/audit"
	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
)

type RouteCatalogStore interface {
	LoadRoutes() (domain.RouteCatalogSnapshot, error)
	SaveRoutes(domain.RouteCatalogSnapshot) error
}

type SessionStore interface {
	LoadSessions() (map[string]any, error)
	SaveSessions(map[string]any) error
}

type ScheduledStore interface {
	LoadScheduled() ([]any, error)
	SaveScheduled([]any) error
}

type DeadLetterStore interface {
	LoadDeadLetters() (map[string]any, error)
	SaveDeadLetters(map[string]any) error
}

type AuditStore interface {
	AppendAudit(audit.Event) (audit.Event, error)
	QueryAudit(audit.Query) ([]audit.Event, error)
}
