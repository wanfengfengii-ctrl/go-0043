package service

import (
	"context"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/protocol"
)

// sendDispatch sends a config dispatch frame to a node's active session. If the
// node has no active session the dispatch is still recorded (so it can be
// delivered on reconnect via the pending-frames replay), but no bytes are
// written immediately.
func (s *Service) sendDispatch(ctx context.Context, rolloutID, nodeID string, cfgVer domain.ConfigVersion, attempt int, direction domain.AttemptDirection) {
	sess := s.sessionFor(nodeID)
	if sess == nil {
		return
	}
	cfg, err := s.store.GetConfig(ctx, cfgVer)
	if err != nil {
		return
	}
	payload := protocol.DispatchPayload{
		RolloutID:     rolloutID,
		ConfigVersion: int64(cfgVer),
		Attempt:       attempt,
		Direction:     direction.String(),
		Content:       cfg.Content,
		Baseline:      direction == domain.AttemptRollback,
	}
	_, _ = sess.Send(protocol.MsgDispatch, payload)
}

// sendNegotiate informs a node of the current target config version when it has
// acknowledged an unknown version.
func (s *Service) sendNegotiate(ctx context.Context, nodeID, rolloutID string, currentConfig int64) {
	sess := s.sessionFor(nodeID)
	if sess == nil {
		return
	}
	payload := protocol.NegotiatePayload{
		RolloutID:     rolloutID,
		CurrentConfig: currentConfig,
	}
	_, _ = sess.Send(protocol.MsgNegotiate, payload)
}
