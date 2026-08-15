package apperr

import (
	"errors"
	"fmt"
)

// Code is a stable, machine-readable error identifier. Error codes are part of
// the public API contract and must never be reused or silently renamed.
type Code string

const (
	// CodeVersionConflict is returned when two concurrent operations compete for
	// the same next config version. The operation is retryable: the caller is
	// expected to reload the baseline and retry.
	CodeVersionConflict Code = "version_conflict"
	// CodeStaleBaseline is returned when a config creation names a baseline
	// version that is not the current head version. Retryable.
	CodeStaleBaseline Code = "stale_baseline"
	// CodePlanRevisionConflict is returned when a plan update carries an
	// expected plan revision that no longer matches the stored value.
	CodePlanRevisionConflict Code = "plan_revision_conflict"
	// CodeUnknownConfigVersion is returned when an acknowledgement references a
	// config version the service has no record of.
	CodeUnknownConfigVersion Code = "unknown_config_version"
	// CodeNotTargetNode is returned when an acknowledgement arrives from a node
	// that is not part of the current stage target set.
	CodeNotTargetNode Code = "not_target_node"
	// CodeNodeMismatch is returned by the session layer when an inbound ack
	// frame carries a node_id that does not match the node identity the session
	// is bound to. A session established for node A must not be able to advance
	// progress by submitting an ack that claims to be from node B; such a frame
	// is rejected at the session boundary before it reaches the business layer.
	CodeNodeMismatch Code = "node_mismatch"
	// CodeStaleAttempt is returned when an acknowledgement targets an attempt
	// that is older than the rollout's current attempt.
	CodeStaleAttempt Code = "stale_attempt"
	// CodeInvalidState is returned when a lifecycle transition is not permitted
	// from the current state (for example pausing a completed rollout).
	CodeInvalidState Code = "invalid_state"
	// CodeRolloutNotFound is returned when a rollout id does not exist.
	CodeRolloutNotFound Code = "rollout_not_found"
	// CodeConfigNotFound is returned when a config version does not exist.
	CodeConfigNotFound Code = "config_not_found"
	// CodeNodeNotFound is returned when a node id does not exist.
	CodeNodeNotFound Code = "node_not_found"
	// CodeGroupNotFound is returned when a group id does not exist.
	CodeGroupNotFound Code = "group_not_found"
	// CodeStageNotFound is returned when a stage index is out of range.
	CodeStageNotFound Code = "stage_not_found"
	// CodeExpectedSequence is returned by the session layer when an incoming
	// frame sequence number skips ahead of the expected value. The expected
	// sequence is attached to the error.
	CodeExpectedSequence Code = "expected_sequence"
	// CodeProtocolConflict is returned when a frame reuses an already-consumed
	// sequence number but carries a different digest.
	CodeProtocolConflict Code = "protocol_conflict"
	// CodeBadRequest is returned for malformed request payloads.
	CodeBadRequest Code = "bad_request"
	// CodeInternal is returned for unexpected internal failures.
	CodeInternal Code = "internal"
)

// Error is the structured error type used across the control plane. It carries
// a stable code, a human-readable message, whether the operation is retryable,
// and an optional cause. The Expected field is used by session errors to expose
// the next expected sequence number to the caller.
type Error struct {
	Code      Code
	Message   string
	Retryable bool
	Expected  int64
	Cause     error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

// New builds a non-retryable error.
func New(code Code, message string) *Error {
	return &Error{Code: code, Message: message}
}

// Newf builds a non-retryable error with a formatted message.
func Newf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Retryable builds a retryable error.
func Retryable(code Code, message string) *Error {
	return &Error{Code: code, Message: message, Retryable: true}
}

// Retryablef builds a retryable error with a formatted message.
func Retryablef(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Retryable: true}
}

// Wrap attaches a cause to an existing error copy.
func (e *Error) Wrap(cause error) *Error {
	cp := *e
	cp.Cause = cause
	return &cp
}

// AsCode extracts the Code from an error, defaulting to CodeInternal.
func AsCode(err error) Code {
	var ae *Error
	if errors.As(err, &ae) {
		return ae.Code
	}
	return CodeInternal
}

// IsRetryable reports whether err is a structured retryable error.
func IsRetryable(err error) bool {
	var ae *Error
	if errors.As(err, &ae) {
		return ae.Retryable
	}
	return false
}

// HasCode reports whether err is a structured error with the given code.
func HasCode(err error, code Code) bool {
	var ae *Error
	if errors.As(err, &ae) {
		return ae.Code == code
	}
	return false
}
