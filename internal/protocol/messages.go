package protocol

import "encoding/json"

// DispatchPayload is sent by the service to instruct a node to apply a config.
// Extra fields are ignored on decode for forward compatibility.
type DispatchPayload struct {
	RolloutID     string `json:"rollout_id"`
	ConfigVersion int64  `json:"config_version"`
	Attempt       int    `json:"attempt"`
	Direction     string `json:"direction"` // "forward" | "rollback"
	Content       string `json:"content"`
	Baseline      bool   `json:"baseline,omitempty"`
}

// AckPayload is sent by a node to acknowledge a config within an attempt.
type AckPayload struct {
	RolloutID     string `json:"rollout_id"`
	NodeID        string `json:"node_id"`
	ConfigVersion int64  `json:"config_version"`
	Attempt       int    `json:"attempt"`
	Level         int    `json:"level"` // 1=received 2=applied 3=confirmed
	OK            bool   `json:"ok"`
	Detail        string `json:"detail,omitempty"`
}

// NegotiatePayload is sent by the service when a node acknowledges an unknown
// config version, telling it the current target version to converge on.
type NegotiatePayload struct {
	RolloutID         string `json:"rollout_id"`
	CurrentConfig     int64  `json:"current_config_version"`
	KnownVersions     []int64 `json:"known_versions,omitempty"`
}

// SessionStartPayload is sent on (re)connect to declare the session watermark.
type SessionStartPayload struct {
	NodeID      string `json:"node_id"`
	LastAckSeq  uint64 `json:"last_ack_seq"`
	ResumeFrom  uint64 `json:"resume_from"`
}

// ProtocolErrorPayload carries a protocol-level error back to the peer.
type ProtocolErrorPayload struct {
	Code             string `json:"code"`
	ExpectedSequence uint64 `json:"expected_sequence,omitempty"`
	Message          string `json:"message,omitempty"`
}

// EncodePayload marshals a payload to JSON.
func EncodePayload(v any) ([]byte, error) {
	return json.Marshal(v)
}

// DecodeDispatch unmarshals a dispatch payload, ignoring unknown fields.
func DecodeDispatch(p []byte) (DispatchPayload, error) {
	var d DispatchPayload
	err := json.Unmarshal(p, &d)
	return d, err
}

// DecodeAck unmarshals an ack payload, ignoring unknown fields.
func DecodeAck(p []byte) (AckPayload, error) {
	var a AckPayload
	err := json.Unmarshal(p, &a)
	return a, err
}

// DecodeSessionStart unmarshals a session-start payload.
func DecodeSessionStart(p []byte) (SessionStartPayload, error) {
	var s SessionStartPayload
	err := json.Unmarshal(p, &s)
	return s, err
}
