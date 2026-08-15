package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/api"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/service"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/testkit"
)

func newAPIServer(t *testing.T) (*api.Server, *testkit.Harness) {
	t.Helper()
	h := testkit.NewHarness(t)
	srv := api.New(h.Service)
	return srv, h
}

func doJSON(t *testing.T, srv *api.Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

func TestAPIHealth(t *testing.T) {
	srv, _ := newAPIServer(t)
	rr := doJSON(t, srv, "GET", "/api/health", nil)
	if rr.Code != 200 {
		t.Fatalf("health status %d", rr.Code)
	}
}

func TestAPIConfigCreateGet(t *testing.T) {
	srv, _ := newAPIServer(t)
	rr := doJSON(t, srv, "POST", "/api/configs", map[string]any{
		"baseline": 0, "content": `{"f":1}`, "request_key": "k1", "created_by": "tester",
	})
	if rr.Code != 200 {
		t.Fatalf("create status %d: %s", rr.Code, rr.Body.String())
	}
	rr2 := doJSON(t, srv, "GET", "/api/configs/1", nil)
	if rr2.Code != 200 {
		t.Fatalf("get status %d: %s", rr2.Code, rr2.Body.String())
	}
}

func TestAPIRolloutLifecycle(t *testing.T) {
	srv, h := newAPIServer(t)
	ctx := context.Background()
	cfg, _, err := h.Service.CreateConfig(ctx, service.CreateConfigRequest{Baseline: 0, Content: `{"f":1}`, RequestKey: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"n1", "n2"} {
		_ = h.Service.UpsertNode(ctx, domain.Node{ID: id, Labels: map[string]string{"z": "a"}, Online: true})
	}
	_, _ = h.Service.UpsertGroup(ctx, domain.Group{ID: "g1", Selector: domain.LabelSelector{MatchLabels: map[string]string{"z": "a"}}})

	rr := doJSON(t, srv, "POST", "/api/rollouts", map[string]any{
		"id": "r1", "target_config_version": cfg.Version, "group_id": "g1",
		"stages": []map[string]any{{
			"name": "s0", "target_type": "count", "count": 2,
			"min_success_rate": 1.0, "max_failures": 0,
			"ack_timeout_ms": 60000, "observation_window_ms": 10000,
		}},
	})
	if rr.Code != 200 {
		t.Fatalf("create rollout %d: %s", rr.Code, rr.Body.String())
	}
	// Start.
	rr = doJSON(t, srv, "POST", "/api/rollouts/r1/start", nil)
	if rr.Code != 200 {
		t.Fatalf("start %d: %s", rr.Code, rr.Body.String())
	}
	// Pause then resume.
	rr = doJSON(t, srv, "POST", "/api/rollouts/r1/pause", nil)
	if rr.Code != 200 {
		t.Fatalf("pause %d: %s", rr.Code, rr.Body.String())
	}
	rr = doJSON(t, srv, "POST", "/api/rollouts/r1/resume", nil)
	if rr.Code != 200 {
		t.Fatalf("resume %d: %s", rr.Code, rr.Body.String())
	}
	// Progress.
	rr = doJSON(t, srv, "GET", "/api/rollouts/r1/progress", nil)
	if rr.Code != 200 {
		t.Fatalf("progress %d: %s", rr.Code, rr.Body.String())
	}
	// Plan update with wrong revision -> conflict.
	rr = doJSON(t, srv, "POST", "/api/rollouts/r1/plan", map[string]any{
		"expected_revision": 99,
		"stage_updates":     map[string]any{},
	})
	if rr.Code != 409 {
		t.Fatalf("expected 409 conflict, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestAPIErrorShape(t *testing.T) {
	srv, _ := newAPIServer(t)
	rr := doJSON(t, srv, "GET", "/api/rollouts/nope/progress", nil)
	if rr.Code != 404 {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	var resp struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error.Code != "rollout_not_found" {
		t.Fatalf("error code = %q, want rollout_not_found", resp.Error.Code)
	}
}
