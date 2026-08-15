package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/apperr"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/store"
)

// CreateConfigRequest creates a new immutable config version from a declared
// baseline.
type CreateConfigRequest struct {
	Baseline   domain.ConfigVersion
	Content    string
	RequestKey string
	CreatedBy  string
}

// CreateConfig persists a new immutable config version. Concurrency rules:
//
//   - If RequestKey is set and a config with that key already exists, the
//     existing config is returned (Created=false) and no new version is
//     created. Replaying a successful request therefore never creates a second
//     version.
//   - The baseline must equal the current head version; otherwise a retryable
//     version_conflict error is returned.
//   - When two different-content requests race for the same next version,
//     exactly one transaction wins; the loser receives a unique-constraint
//     failure that is translated into a retryable version_conflict.
func (s *Service) CreateConfig(ctx context.Context, req CreateConfigRequest) (domain.FeatureConfig, bool, error) {
	now := s.clk.Now()
	res, err := s.store.CreateConfig(ctx, req.Baseline, req.Content, req.RequestKey, req.CreatedBy, now)
	if err != nil {
		if store.IsStaleBaseline(err) {
			return domain.FeatureConfig{}, false, apperr.Retryable(apperr.CodeVersionConflict,
				fmt.Sprintf("baseline %d is not the current head", req.Baseline))
		}
		if store.IsUniqueVersion(err) {
			return domain.FeatureConfig{}, false, apperr.Retryable(apperr.CodeVersionConflict,
				"another transaction created this version concurrently")
		}
		return domain.FeatureConfig{}, false, err
	}
	return res.Config, res.Created, nil
}

// GetConfig returns a config by version.
func (s *Service) GetConfig(ctx context.Context, version domain.ConfigVersion) (domain.FeatureConfig, error) {
	c, err := s.store.GetConfig(ctx, version)
	if errors.Is(err, store.ErrNotFound) {
		return c, apperr.New(apperr.CodeConfigNotFound, "config version not found")
	}
	return c, err
}

// HeadVersion returns the current maximum config version.
func (s *Service) HeadVersion(ctx context.Context) (domain.ConfigVersion, error) {
	return s.store.HeadVersion(ctx)
}

// ListConfigs returns all configs ordered by version.
func (s *Service) ListConfigs(ctx context.Context) ([]domain.FeatureConfig, error) {
	return s.store.ListConfigs(ctx)
}
