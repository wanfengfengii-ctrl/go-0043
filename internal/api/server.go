// Package api implements the JSON management API of the control service. It is
// a thin transport layer over the service: handlers parse requests, call the
// service and write structured JSON responses. Every write accepts an optional
// X-Idempotency-Key header (replays return the original response) and, where
// relevant, an X-Expected-Revision header for optimistic concurrency. Errors
// are returned with a stable code, message and retryable flag.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/apperr"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/service"
)

// Server is the HTTP management API.
type Server struct {
	svc      *service.Service
	mux      *http.ServeMux
}

// New builds a Server and registers routes.
func New(svc *service.Service) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.routes()
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Handler returns the http.Handler for embedding in a larger server.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	m := s.mux
	m.HandleFunc("GET /api/health", s.handleHealth)

	m.HandleFunc("GET /api/configs", s.handleListConfigs)
	m.HandleFunc("POST /api/configs", s.handleCreateConfig)
	m.HandleFunc("GET /api/configs/{version}", s.handleGetConfig)

	m.HandleFunc("GET /api/nodes", s.handleListNodes)
	m.HandleFunc("POST /api/nodes", s.handleUpsertNode)
	m.HandleFunc("GET /api/nodes/{id}", s.handleGetNode)

	m.HandleFunc("GET /api/groups", s.handleListGroups)
	m.HandleFunc("POST /api/groups", s.handleUpsertGroup)
	m.HandleFunc("GET /api/groups/{id}", s.handleGetGroup)

	m.HandleFunc("POST /api/rollouts", s.handleCreateRollout)
	m.HandleFunc("GET /api/rollouts/{id}", s.handleGetRollout)
	m.HandleFunc("POST /api/rollouts/{id}/start", s.handleStartRollout)
	m.HandleFunc("POST /api/rollouts/{id}/plan", s.handleUpdatePlan)
	m.HandleFunc("POST /api/rollouts/{id}/pause", s.handlePause)
	m.HandleFunc("POST /api/rollouts/{id}/resume", s.handleResume)
	m.HandleFunc("POST /api/rollouts/{id}/rollback", s.handleRollback)
	m.HandleFunc("GET /api/rollouts/{id}/progress", s.handleProgress)
	m.HandleFunc("GET /api/rollouts/{id}/audit", s.handleAudit)
	m.HandleFunc("POST /api/rollouts/{id}/acks", s.handleSubmitAck)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

// ---------- Configs ----------

type createConfigReq struct {
	Baseline   int64  `json:"baseline"`
	Content    string `json:"content"`
	RequestKey string `json:"request_key"`
	CreatedBy  string `json:"created_by"`
}

func (s *Server) handleCreateConfig(w http.ResponseWriter, r *http.Request) {
	var req createConfigReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, err.Error()))
		return
	}
	cfg, created, err := s.svc.CreateConfig(r.Context(), service.CreateConfigRequest{
		Baseline:   domain.ConfigVersion(req.Baseline),
		Content:    req.Content,
		RequestKey: req.RequestKey,
		CreatedBy:  req.CreatedBy,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"config": cfg, "created": created})
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	v, err := strconv.ParseInt(r.PathValue("version"), 10, 64)
	if err != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, "invalid version"))
		return
	}
	cfg, err := s.svc.GetConfig(r.Context(), domain.ConfigVersion(v))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"config": cfg})
}

func (s *Server) handleListConfigs(w http.ResponseWriter, r *http.Request) {
	cfgs, err := s.svc.ListConfigs(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"configs": cfgs})
}

// ---------- Nodes / Groups ----------

func (s *Server) handleUpsertNode(w http.ResponseWriter, r *http.Request) {
	var n domain.Node
	if err := decodeBody(r, &n); err != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, err.Error()))
		return
	}
	if err := s.svc.UpsertNode(r.Context(), n); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": n})
}

func (s *Server) handleGetNode(w http.ResponseWriter, r *http.Request) {
	n, err := s.svc.GetNode(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": n})
}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	ns, err := s.svc.ListNodes(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": ns})
}

func (s *Server) handleUpsertGroup(w http.ResponseWriter, r *http.Request) {
	var g domain.Group
	if err := decodeBody(r, &g); err != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, err.Error()))
		return
	}
	g2, err := s.svc.UpsertGroup(r.Context(), g)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"group": g2})
}

func (s *Server) handleGetGroup(w http.ResponseWriter, r *http.Request) {
	g, err := s.svc.GetGroup(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"group": g})
}

func (s *Server) handleListGroups(w http.ResponseWriter, r *http.Request) {
	gs, err := s.svc.ListGroups(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"groups": gs})
}

// ---------- Rollouts ----------

type stageSpecReq struct {
	Name              string  `json:"name"`
	TargetType        string  `json:"target_type"`
	Count             int     `json:"count"`
	Percent           int     `json:"percent"`
	MinSuccessRate    float64 `json:"min_success_rate"`
	MaxFailures       int     `json:"max_failures"`
	AckTimeoutMS      int     `json:"ack_timeout_ms"`
	ObservationWindowMS int   `json:"observation_window_ms"`
}

type createRolloutReq struct {
	ID                string         `json:"id"`
	TargetConfigVersion int64        `json:"target_config_version"`
	GroupID           string         `json:"group_id"`
	Stages            []stageSpecReq `json:"stages"`
}

func toStageSpec(s stageSpecReq) service.StageSpec {
	return service.StageSpec{
		Name:              s.Name,
		Target:            domain.StageTarget{Kind: domain.StageTargetKind(s.TargetType), Count: s.Count, Percent: s.Percent},
		MinSuccessRate:    s.MinSuccessRate,
		MaxFailures:       s.MaxFailures,
		AckTimeout:        time.Duration(s.AckTimeoutMS) * time.Millisecond,
		ObservationWindow: time.Duration(s.ObservationWindowMS) * time.Millisecond,
	}
}

func (s *Server) handleCreateRollout(w http.ResponseWriter, r *http.Request) {
	var req createRolloutReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, err.Error()))
		return
	}
	stages := make([]service.StageSpec, len(req.Stages))
	for i, sp := range req.Stages {
		stages[i] = toStageSpec(sp)
	}
	roll, err := s.svc.CreateRollout(r.Context(), service.CreateRolloutRequest{
		ID:                   req.ID,
		TargetConfigVersion:  domain.ConfigVersion(req.TargetConfigVersion),
		GroupID:              req.GroupID,
		Stages:               stages,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rollout": roll})
}

func (s *Server) handleGetRollout(w http.ResponseWriter, r *http.Request) {
	roll, err := s.svc.GetRollout(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rollout": roll})
}

func (s *Server) handleStartRollout(w http.ResponseWriter, r *http.Request) {
	roll, err := s.svc.StartRollout(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rollout": roll})
}

type updatePlanReq struct {
	ExpectedRevision int64          `json:"expected_revision"`
	StageUpdates     map[int]stageSpecReq `json:"stage_updates"`
	AddStages        []stageSpecReq `json:"add_stages"`
}

func (s *Server) handleUpdatePlan(w http.ResponseWriter, r *http.Request) {
	var req updatePlanReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, err.Error()))
		return
	}
	updates := make(map[int]service.StageSpec, len(req.StageUpdates))
	for k, sp := range req.StageUpdates {
		updates[k] = toStageSpec(sp)
	}
	adds := make([]service.StageSpec, len(req.AddStages))
	for i, sp := range req.AddStages {
		adds[i] = toStageSpec(sp)
	}
	roll, err := s.svc.UpdatePlan(r.Context(), r.PathValue("id"), req.ExpectedRevision, service.PlanChange{
		StageUpdates: updates, AddStages: adds,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rollout": roll})
}

func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	roll, err := s.svc.Pause(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rollout": roll})
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	roll, err := s.svc.Resume(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rollout": roll})
}

func (s *Server) handleRollback(w http.ResponseWriter, r *http.Request) {
	roll, err := s.svc.Rollback(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rollout": roll})
}

func (s *Server) handleProgress(w http.ResponseWriter, r *http.Request) {
	p, err := s.svc.GetProgress(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"progress": p})
}

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	audits, err := s.svc.ListAuditAcks(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"audits": audits})
}

type submitAckReq struct {
	RolloutID     string `json:"rollout_id"`
	NodeID        string `json:"node_id"`
	ConfigVersion int64  `json:"config_version"`
	Attempt       int    `json:"attempt"`
	Level         int    `json:"level"`
}

func (s *Server) handleSubmitAck(w http.ResponseWriter, r *http.Request) {
	var req submitAckReq
	if err := decodeBody(r, &req); err != nil {
		writeError(w, apperr.New(apperr.CodeBadRequest, err.Error()))
		return
	}
	if req.RolloutID == "" {
		req.RolloutID = r.PathValue("id")
	}
	ack := domain.Ack{
		RolloutID:     req.RolloutID,
		NodeID:        req.NodeID,
		ConfigVersion: domain.ConfigVersion(req.ConfigVersion),
		Attempt:       req.Attempt,
		Level:         domain.AckLevel(req.Level),
	}
	out, err := s.svc.ProcessAck(r.Context(), ack)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"applied":       out.Applied,
		"duplicate":     out.Duplicate,
		"audit_reason":  string(out.AuditReason),
		"new_highwater": out.NewHighWater,
	})
}

// ---------- helpers ----------

func decodeBody(r *http.Request, v any) error {
	ct := r.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(ct, "application/json") {
		return fmt.Errorf("unsupported content type %q", ct)
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	ae := &apperr.Error{Code: apperr.CodeInternal, Message: err.Error()}
	if !errors.As(err, &ae) {
		ae.Code = apperr.CodeInternal
		ae.Message = err.Error()
	}
	status := httpStatusFor(ae.Code)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":      string(ae.Code),
			"message":   ae.Message,
			"retryable": ae.Retryable,
			"expected":  ae.Expected,
		},
	})
}

func httpStatusFor(c apperr.Code) int {
	switch c {
	case apperr.CodeBadRequest:
		return http.StatusBadRequest
	case apperr.CodeConfigNotFound, apperr.CodeRolloutNotFound, apperr.CodeNodeNotFound, apperr.CodeGroupNotFound, apperr.CodeStageNotFound:
		return http.StatusNotFound
	case apperr.CodeVersionConflict, apperr.CodePlanRevisionConflict, apperr.CodeStaleBaseline, apperr.CodeStaleAttempt, apperr.CodeProtocolConflict, apperr.CodeExpectedSequence:
		return http.StatusConflict
	case apperr.CodeInvalidState, apperr.CodeUnknownConfigVersion, apperr.CodeNotTargetNode:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}
