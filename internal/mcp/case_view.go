package mcp

import (
	"context"
	"fmt"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// caseView is a local materialized view for mutable case records.
// It keeps one in-memory identity map entry per case with dirty-field tracking.
type caseView struct {
	snapshot *domain.Case
	local    *domain.Case
	dirty    map[string]bool
}

func (h *handler) rememberCase(c *domain.Case) {
	if c == nil {
		return
	}
	h.caseViewMu.Lock()
	defer h.caseViewMu.Unlock()
	if h.caseViews == nil {
		h.caseViews = make(map[string]*caseView)
	}
	h.caseViews[c.ID] = &caseView{
		snapshot: cloneCase(c),
		local:    cloneCase(c),
		dirty:    make(map[string]bool),
	}
}

func (h *handler) getOrMaterializeCaseView(ctx context.Context, caseID string) (*caseView, error) {
	h.caseViewMu.Lock()
	if h.caseViews == nil {
		h.caseViews = make(map[string]*caseView)
	}
	if v, ok := h.caseViews[caseID]; ok {
		h.caseViewMu.Unlock()
		return v, nil
	}
	h.caseViewMu.Unlock()

	c, err := h.store.GetCase(ctx, caseID)
	if err != nil {
		return nil, err
	}
	v := &caseView{
		snapshot: cloneCase(c),
		local:    cloneCase(c),
		dirty:    make(map[string]bool),
	}
	h.caseViewMu.Lock()
	h.caseViews[caseID] = v
	h.caseViewMu.Unlock()
	return v, nil
}

func (h *handler) stageCaseMutation(ctx context.Context, caseID string, mutate func(c *domain.Case), dirtyFields ...string) (*domain.Case, error) {
	v, err := h.getOrMaterializeCaseView(ctx, caseID)
	if err != nil {
		return nil, err
	}
	h.caseViewMu.Lock()
	defer h.caseViewMu.Unlock()
	mutate(v.local)
	for _, f := range dirtyFields {
		v.dirty[f] = true
	}
	return cloneCase(v.local), nil
}

func (h *handler) flushCaseView(ctx context.Context, caseID string) error {
	v, err := h.getOrMaterializeCaseView(ctx, caseID)
	if err != nil {
		return err
	}
	h.caseViewMu.Lock()
	if len(v.dirty) == 0 {
		h.caseViewMu.Unlock()
		return nil
	}
	local := cloneCase(v.local)
	snapshot := cloneCase(v.snapshot)
	h.caseViewMu.Unlock()

	current, err := h.store.GetCase(ctx, caseID)
	if err != nil {
		return err
	}
	if !sameCase(current, snapshot) {
		return fmt.Errorf("case %q local view is stale: %w", caseID, domain.ErrConflict)
	}
	if err := h.store.PutCase(ctx, local); err != nil {
		return err
	}
	h.caseViewMu.Lock()
	v.snapshot = cloneCase(local)
	v.local = cloneCase(local)
	v.dirty = make(map[string]bool)
	h.caseViewMu.Unlock()
	return nil
}

func cloneCase(c *domain.Case) *domain.Case {
	if c == nil {
		return nil
	}
	out := *c
	if c.ClosedAt != nil {
		closedAt := *c.ClosedAt
		out.ClosedAt = &closedAt
	}
	return &out
}

func sameCase(a, b *domain.Case) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.ID != b.ID || a.Title != b.Title || a.Status != b.Status || !a.CreatedAt.Equal(b.CreatedAt) {
		return false
	}
	switch {
	case a.ClosedAt == nil && b.ClosedAt == nil:
		return true
	case a.ClosedAt == nil || b.ClosedAt == nil:
		return false
	default:
		return a.ClosedAt.Equal(*b.ClosedAt)
	}
}
