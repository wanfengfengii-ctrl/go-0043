package service

import (
	"context"
	"errors"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/apperr"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/store"
)

// UpsertNode registers or updates a node. New nodes are marked offline until a
// session connects.
func (s *Service) UpsertNode(ctx context.Context, n domain.Node) error {
	if n.ID == "" {
		return apperr.New(apperr.CodeBadRequest, "node id is required")
	}
	if n.LastSeen.IsZero() {
		n.LastSeen = s.clk.Now()
	}
	return s.store.UpsertNode(ctx, n)
}

// GetNode returns a node by ID.
func (s *Service) GetNode(ctx context.Context, id string) (domain.Node, error) {
	n, err := s.store.GetNode(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return n, apperr.New(apperr.CodeNodeNotFound, "node not found")
	}
	return n, err
}

// ListNodes returns all nodes.
func (s *Service) ListNodes(ctx context.Context) ([]domain.Node, error) {
	return s.store.ListNodes(ctx)
}

// UpsertGroup creates or updates a group, recomputing its membership from the
// current node set deterministically. MemberIDs are sorted.
func (s *Service) UpsertGroup(ctx context.Context, g domain.Group) (domain.Group, error) {
	if g.ID == "" {
		return g, apperr.New(apperr.CodeBadRequest, "group id is required")
	}
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return g, err
	}
	var members []string
	for _, n := range nodes {
		if g.Selector.Matches(n.Labels) {
			members = append(members, n.ID)
		}
	}
	g.MemberIDs = domain.SortMemberIDs(members)
	if g.UpdatedAt.IsZero() {
		g.UpdatedAt = s.clk.Now()
	}
	return s.store.UpsertGroup(ctx, g)
}

// GetGroup returns a group by ID.
func (s *Service) GetGroup(ctx context.Context, id string) (domain.Group, error) {
	g, err := s.store.GetGroup(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return g, apperr.New(apperr.CodeGroupNotFound, "group not found")
	}
	return g, err
}

// ListGroups returns all groups.
func (s *Service) ListGroups(ctx context.Context) ([]domain.Group, error) {
	return s.store.ListGroups(ctx)
}

// RecomputeGroupMembers recomputes a group's membership from the current node
// set without changing the selector. Used when nodes are added/removed. Returns
// the new revision.
func (s *Service) RecomputeGroupMembers(ctx context.Context, groupID string) (domain.Group, error) {
	g, err := s.store.GetGroup(ctx, groupID)
	if errors.Is(err, store.ErrNotFound) {
		return g, apperr.New(apperr.CodeGroupNotFound, "group not found")
	}
	if err != nil {
		return g, err
	}
	return s.UpsertGroup(ctx, g)
}
