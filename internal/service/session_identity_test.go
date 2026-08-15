package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/protocol"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/service"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/testkit"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/vnode"
)

func TestSessionRejectsAckForDifferentNodeIdentity(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()

	cfg, _, err := h.Service.CreateConfig(ctx, service.CreateConfigRequest{
		Baseline: 0, Content: `{"feature":true}`, RequestKey: "identity-config",
	})
	if err != nil {
		t.Fatalf("create config: %v", err)
	}
	if err := h.Service.UpsertNode(ctx, domain.Node{ID: "attacker", Labels: map[string]string{"role": "other"}, Online: true}); err != nil {
		t.Fatalf("upsert attacker: %v", err)
	}
	if err := h.Service.UpsertNode(ctx, domain.Node{ID: "victim", Labels: map[string]string{"role": "target"}, Online: true}); err != nil {
		t.Fatalf("upsert victim: %v", err)
	}
	if _, err := h.Service.UpsertGroup(ctx, domain.Group{
		ID: "victim-only", Selector: domain.LabelSelector{MatchLabels: map[string]string{"role": "target"}},
	}); err != nil {
		t.Fatalf("upsert group: %v", err)
	}

	attackerWire, attackerServiceWire := vnode.Connect()
	attackerSession, err := h.Service.AcceptSession(ctx, "attacker", attackerServiceWire)
	if err != nil {
		t.Fatalf("accept attacker session: %v", err)
	}
	r, err := h.Service.CreateRollout(ctx, service.CreateRolloutRequest{
		ID: "identity-rollout", TargetConfigVersion: cfg.Version, GroupID: "victim-only",
		Stages: []service.StageSpec{{
			Name: "victim-stage", Target: domain.StageTarget{Kind: domain.TargetCount, Count: 1},
			MinSuccessRate: 1, MaxFailures: 0, AckTimeout: time.Minute, ObservationWindow: time.Minute,
		}},
	})
	if err != nil {
		t.Fatalf("create rollout: %v", err)
	}
	if _, err := h.Service.StartRollout(ctx, r.ID); err != nil {
		t.Fatalf("start rollout: %v", err)
	}

	before, err := h.Service.GetRollout(ctx, r.ID)
	if err != nil {
		t.Fatalf("get rollout before spoof: %v", err)
	}
	if before.Stages[0].State != domain.StageDispatching || before.Stages[0].SuccessCount != 0 {
		t.Fatalf("unexpected initial stage: state=%s success=%d", before.Stages[0].State, before.Stages[0].SuccessCount)
	}

	spoofPayload, err := json.Marshal(protocol.AckPayload{
		RolloutID: r.ID, NodeID: "victim", ConfigVersion: int64(cfg.Version), Attempt: 1,
		Level: int(domain.AckConfirmed), OK: true,
	})
	if err != nil {
		t.Fatalf("marshal spoof ack: %v", err)
	}
	spoofFrame, err := protocol.Encode(protocol.Frame{
		SessionID: uint64(attackerSession.SessionID()), Sequence: 1,
		MessageType: protocol.MsgAck, Payload: spoofPayload,
	})
	if err != nil {
		t.Fatalf("encode spoof ack: %v", err)
	}
	if _, err := attackerWire.Write(spoofFrame); err != nil {
		t.Fatalf("write spoof ack: %v", err)
	}
	if _, err := attackerSession.PumpIn(); err == nil {
		t.Fatal("spoofed ack was accepted")
	}

	after, err := h.Service.GetRollout(ctx, r.ID)
	if err != nil {
		t.Fatalf("get rollout after spoof: %v", err)
	}
	if after.Stages[0].State != domain.StageDispatching || after.Stages[0].SuccessCount != 0 {
		t.Fatalf("spoofed ack changed stage: state=%s success=%d", after.Stages[0].State, after.Stages[0].SuccessCount)
	}
	sessRow, err := h.Store.GetSession(ctx, attackerSession.SessionID())
	if err != nil {
		t.Fatalf("get attacker session: %v", err)
	}
	if sessRow.ExpectedSeq != 1 {
		t.Fatalf("spoofed ack advanced sequence to %d, want 1", sessRow.ExpectedSeq)
	}

	victimWire, victimServiceWire := vnode.Connect()
	victimSession, err := h.Service.AcceptSession(ctx, "victim", victimServiceWire)
	if err != nil {
		t.Fatalf("accept victim session: %v", err)
	}
	validPayload, err := json.Marshal(protocol.AckPayload{
		RolloutID: r.ID, NodeID: "victim", ConfigVersion: int64(cfg.Version), Attempt: 1,
		Level: int(domain.AckConfirmed), OK: true,
	})
	if err != nil {
		t.Fatalf("marshal valid ack: %v", err)
	}
	validFrame, err := protocol.Encode(protocol.Frame{
		SessionID: uint64(victimSession.SessionID()), Sequence: 1,
		MessageType: protocol.MsgAck, Payload: validPayload,
	})
	if err != nil {
		t.Fatalf("encode valid ack: %v", err)
	}
	if _, err := victimWire.Write(validFrame); err != nil {
		t.Fatalf("write valid ack: %v", err)
	}
	if _, err := victimSession.PumpIn(); err != nil {
		t.Fatalf("valid ack rejected: %v", err)
	}

	afterValid, err := h.Service.GetRollout(ctx, r.ID)
	if err != nil {
		t.Fatalf("get rollout after valid ack: %v", err)
	}
	if afterValid.Stages[0].State != domain.StageObserving || afterValid.Stages[0].SuccessCount != 1 {
		t.Fatalf("valid ack did not advance stage: state=%s success=%d", afterValid.Stages[0].State, afterValid.Stages[0].SuccessCount)
	}
}
