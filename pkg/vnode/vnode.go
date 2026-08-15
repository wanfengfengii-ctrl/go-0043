// Package vnode implements a programmable virtual edge node used by the test
// suite and as a standalone CLI. A virtual node wraps one end of an in-memory
// transport.Wire, decodes dispatch and negotiate frames from the service, and
// emits acknowledgement frames according to a configurable program.
//
// Fault-injection knobs let a test exercise the protocol and state-machine edge
// cases required by the acceptance criteria: delayed acknowledgements,
// duplicate acks, wrong config versions, sequence jumps, digest corruption and
// rollback refusal. Everything is synchronous and driven by Pump, so tests
// never depend on real network timing.
package vnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/protocol"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/transport"
)

// Behavior configures how a virtual node responds to dispatches.
type Behavior struct {
	// AckLevel is the level the node acknowledges at. Default AckConfirmed.
	AckLevel domain.AckLevel
	// DuplicateAck makes the node send each ack twice.
	DuplicateAck bool
	// WrongVersion makes the node acknowledge a different (non-target) config
	// version, exercising the audit/negotiation path.
	WrongVersion domain.ConfigVersion
	// SeqJump makes the node emit acks with a jumped sequence number, exercising
	// the expected_sequence path. If 0, the node uses its own monotonic seq.
	SeqJump uint64
	// CorruptDigest makes the node send a frame whose digest does not match.
	CorruptDigest bool
	// RefuseRollback makes the node ignore rollback (baseline) dispatches,
	// never acknowledging them.
	RefuseRollback bool
	// RefuseForward makes the node ignore forward dispatches.
	RefuseForward bool
	// NackInstead makes the node send an ack with OK=false (a failure) rather
	// than a confirmation.
	NackInstead bool
	// UnknownVersion makes the node acknowledge a config version the service
	// has no record of.
	UnknownVersion domain.ConfigVersion
}

// VNode is a programmable virtual node.
type VNode struct {
	mu       sync.Mutex
	id       string
	wire     transport.Wire
	decoder  *protocol.Decoder
	beh      Behavior
	outSeq   uint64
	received []protocol.Frame
	dispatches []protocol.DispatchPayload
	acks     int
}

// New creates a virtual node on the given wire.
func New(id string, wire transport.Wire, beh Behavior) *VNode {
	v := &VNode{id: id, wire: wire, beh: beh}
	v.decoder = protocol.NewDecoder(protocol.DefaultMaxFrame, v.handleFrame)
	return v
}

// ID returns the node id.
func (v *VNode) ID() string { return v.id }

// SetBehavior replaces the behavior at runtime.
func (v *VNode) SetBehavior(beh Behavior) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.beh = beh
}

// Pump reads bytes available from the service and processes them. It is
// non-blocking and returns when no more bytes are available. Returns the number
// of frames processed.
func (v *VNode) Pump() (int, error) {
	var n int
	for {
		chunk, err := v.wire.ReadChunk()
		if len(chunk) == 0 {
			return n, nil
		}
		if err != nil && !errors.Is(err, nil) {
			// EOF on closed wire; ignore and stop.
			return n, nil
		}
		if err := v.decoder.Feed(chunk); err != nil {
			return n, err
		}
		n++
	}
}

// handleFrame processes a dispatch or negotiate frame from the service.
func (v *VNode) handleFrame(f protocol.Frame) error {
	v.mu.Lock()
	v.received = append(v.received, f)
	v.mu.Unlock()
	switch f.MessageType {
	case protocol.MsgDispatch:
		var dp protocol.DispatchPayload
		if err := json.Unmarshal(f.Payload, &dp); err != nil {
			return nil
		}
		v.mu.Lock()
		v.dispatches = append(v.dispatches, dp)
		beh := v.beh
		v.mu.Unlock()
		if beh.RefuseForward && !dp.Baseline {
			return nil
		}
		if beh.RefuseRollback && dp.Baseline {
			return nil
		}
		v.respond(dp, beh)
	case protocol.MsgNegotiate:
		// The service is informing us of the current version; a real node would
		// converge. Nothing to do for the test harness.
	}
	return nil
}

// respond emits the node's acknowledgement according to its behavior.
func (v *VNode) respond(dp protocol.DispatchPayload, beh Behavior) {
	ackVer := domain.ConfigVersion(dp.ConfigVersion)
	if beh.WrongVersion != 0 {
		ackVer = beh.WrongVersion
	}
	if beh.UnknownVersion != 0 {
		ackVer = beh.UnknownVersion
	}
	level := beh.AckLevel
	if level == 0 {
		level = domain.AckConfirmed
	}
	ok := true
	if beh.NackInstead {
		ok = false
		level = domain.AckReceived
	}
	ap := protocol.AckPayload{
		RolloutID:     dp.RolloutID,
		NodeID:        v.id,
		ConfigVersion: int64(ackVer),
		Attempt:       dp.Attempt,
		Level:         int(level),
		OK:            ok,
	}
	v.sendAck(ap, beh)
	if beh.DuplicateAck {
		v.sendAck(ap, beh)
	}
}

func (v *VNode) sendAck(ap protocol.AckPayload, beh Behavior) {
	v.mu.Lock()
	v.outSeq++
	seq := v.outSeq
	v.acks++
	v.mu.Unlock()
	if beh.SeqJump != 0 {
		seq = beh.SeqJump
	}
	frame := protocol.Frame{
		Major:       protocol.SupportedMajor,
		Minor:       protocol.CurrentMinor,
		SessionID:   0,
		Sequence:    seq,
		MessageType: protocol.MsgAck,
	}
	payload, err := json.Marshal(ap)
	if err != nil {
		return
	}
	frame.Payload = payload
	encoded, err := protocol.Encode(frame)
	if err != nil {
		return
	}
	if beh.CorruptDigest {
		// Flip a byte in the payload (after the digest header) so the digest no
		// longer matches, while keeping the header intact.
		if len(encoded) > protocol.HeaderSize {
			encoded[protocol.HeaderSize] ^= 0xFF
		}
	}
	_, _ = v.wire.Write(encoded)
}

// SendRaw writes an arbitrary pre-encoded frame to the service. Used by tests
// to craft adversarial frames (bad sequence, bad digest) directly.
func (v *VNode) SendRaw(frame protocol.Frame) error {
	encoded, err := protocol.Encode(frame)
	if err != nil {
		return err
	}
	_, err = v.wire.Write(encoded)
	return err
}

// SendRawBytes writes raw bytes directly to the wire (for corrupt/malformed
// frame tests).
func (v *VNode) SendRawBytes(b []byte) (int, error) {
	return v.wire.Write(b)
}

// Dispatches returns a copy of the dispatch payloads the node has received.
func (v *VNode) Dispatches() []protocol.DispatchPayload {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]protocol.DispatchPayload, len(v.dispatches))
	copy(out, v.dispatches)
	return out
}

// AcksSent returns the number of ack frames the node has sent.
func (v *VNode) AcksSent() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.acks
}

// Close closes the node's wire.
func (v *VNode) Close() error { return v.wire.Close() }

// Connect returns a connected (node, service-side) wire pair.
func Connect() (nodeWire, serviceWire transport.Wire) {
	return transport.NewDuplex()
}

// String describes the node for diagnostics.
func (v *VNode) String() string {
	return fmt.Sprintf("vnode(%s)", v.id)
}
