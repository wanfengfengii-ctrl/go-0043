// Package protocol implements the length-prefixed binary envelope used on the
// node streaming channel.
//
// Wire format (all integers big-endian). HeaderSize is fixed so an incremental
// decoder knows exactly how many bytes to accumulate before the length field is
// available:
//
//	[ 0:  4]  Magic "EFFP"
//	[ 4:  5]  Major version   (uint8)
//	[ 5:  6]  Minor version   (uint8)
//	[ 6: 14]  SessionID       (uint64)
//	[14: 22]  Sequence        (uint64)
//	[22: 24]  MessageType     (uint16)
//	[24: 28]  PayloadLength   (uint32)
//	[28: 60]  Digest          (32 bytes, SHA-256 of Payload)
//	[60: 60+PayloadLength]  Payload (UTF-8 JSON)
//
// The decoder tolerates arbitrary split (拆包) and sticky (粘包) byte
// boundaries, rejects unsupported major versions, oversize frames, digest
// mismatches, unknown message types and invalid JSON payloads. None of these
// failures write to the database because they surface before a frame is
// delivered to the business layer.
package protocol

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Magic marks the start of every frame.
var Magic = [4]byte{'E', 'F', 'F', 'P'}

// SupportedMajor is the only major version the service understands. A higher
// major version is an incompatible, breaking change and is rejected outright.
const SupportedMajor uint8 = 1

// CurrentMinor is the highest minor version the service emits. Incoming frames
// with any minor are accepted (forward-compatible); extra payload fields are
// ignored.
const CurrentMinor uint8 = 2

const (
	// HeaderSize is the fixed size of the envelope header including the digest.
	HeaderSize = 60
	// DefaultMaxFrame is the default cap on a single complete frame (header +
	// payload). Frames whose payload length would exceed this are rejected.
	DefaultMaxFrame = 1 << 20 // 1 MiB
)

// MessageType identifies the payload schema.
type MessageType uint16

const (
	MsgDispatch     MessageType = 1 // service -> node: apply a config
	MsgAck          MessageType = 2 // node -> service: acknowledge a config
	MsgNegotiate    MessageType = 3 // service -> node: unknown-version negotiation
	MsgHeartbeat    MessageType = 4
	MsgSessionStart MessageType = 5
	MsgProtocolError MessageType = 6 // expected_sequence / protocol_conflict
)

// Known reports whether the message type is recognised.
func (m MessageType) Known() bool {
	switch m {
	case MsgDispatch, MsgAck, MsgNegotiate, MsgHeartbeat, MsgSessionStart, MsgProtocolError:
		return true
	}
	return false
}

// Frame is a decoded envelope.
type Frame struct {
	Major       uint8
	Minor       uint8
	SessionID   uint64
	Sequence    uint64
	MessageType MessageType
	Payload     []byte
}

// ProtocolError is a typed error returned by the decoder. It is asserted on by
// tests via errors.Is / errors.As.
type ProtocolError struct {
	Kind ErrorKind
	Msg  string
}

// ErrorKind enumerates decoder failure modes.
type ErrorKind string

const (
	ErrBadMagic              ErrorKind = "bad_magic"
	ErrUnsupportedMajor      ErrorKind = "unsupported_major_version"
	ErrUnknownMessageType    ErrorKind = "unknown_message_type"
	ErrPayloadTooLarge       ErrorKind = "payload_too_large"
	ErrDigestMismatch        ErrorKind = "digest_mismatch"
	ErrInvalidJSON           ErrorKind = "invalid_json"
	ErrShortFrame            ErrorKind = "short_frame"
)

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("protocol %s: %s", e.Kind, e.Msg)
}

// Is supports errors.Is against an ErrorKind.
func (e *ProtocolError) Is(target error) bool {
	if pe, ok := target.(*ProtocolError); ok {
		return e.Kind == pe.Kind
	}
	return false
}

// KindOf extracts the ErrorKind from a decoder error, returning ok=false if the
// error is not a protocol error.
func KindOf(err error) (ErrorKind, bool) {
	var pe *ProtocolError
	if errors.As(err, &pe) {
		return pe.Kind, true
	}
	return "", false
}

// IsKind reports whether err is a protocol error of the given kind.
func IsKind(err error, kind ErrorKind) bool {
	k, ok := KindOf(err)
	return ok && k == kind
}

// digestOf returns the SHA-256 of the payload, which is what the header digest
// commits to.
func digestOf(payload []byte) [32]byte {
	return sha256.Sum256(payload)
}

// Encode builds the on-the-wire bytes for a frame at the current protocol
// version. The payload must already be valid JSON; callers should construct it
// via json.Marshal.
func Encode(f Frame) ([]byte, error) {
	if f.Major == 0 {
		f.Major = SupportedMajor
	}
	if f.Minor == 0 {
		f.Minor = CurrentMinor
	}
	if uint32(len(f.Payload)) > DefaultMaxFrame-HeaderSize {
		return nil, &ProtocolError{Kind: ErrPayloadTooLarge, Msg: fmt.Sprintf("payload %d exceeds limit", len(f.Payload))}
	}
	if f.MessageType != 0 && !f.MessageType.Known() {
		return nil, &ProtocolError{Kind: ErrUnknownMessageType, Msg: fmt.Sprintf("message type %d", f.MessageType)}
	}
	digest := digestOf(f.Payload)
	buf := make([]byte, HeaderSize+len(f.Payload))
	copy(buf[0:4], Magic[:])
	buf[4] = f.Major
	buf[5] = f.Minor
	binary.BigEndian.PutUint64(buf[6:14], f.SessionID)
	binary.BigEndian.PutUint64(buf[14:22], f.Sequence)
	binary.BigEndian.PutUint16(buf[22:24], uint16(f.MessageType))
	binary.BigEndian.PutUint32(buf[24:28], uint32(len(f.Payload)))
	copy(buf[28:60], digest[:])
	copy(buf[60:], f.Payload)
	return buf, nil
}

// MustEncode panics if Encode fails. Intended for tests.
func MustEncode(f Frame) []byte {
	b, err := Encode(f)
	if err != nil {
		panic(err)
	}
	return b
}

// ReadFrame reads a single frame from r, blocking until a complete frame is
// available or an error occurs. It performs the same validation as the
// streaming Decoder.
func ReadFrame(r io.Reader) (*Frame, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	return parseFromHeader(r, header, DefaultMaxFrame)
}

func parseFromHeader(r io.Reader, header []byte, maxFrame int) (*Frame, error) {
	if header[0] != Magic[0] || header[1] != Magic[1] || header[2] != Magic[2] || header[3] != Magic[3] {
		return nil, &ProtocolError{Kind: ErrBadMagic, Msg: "magic bytes do not match"}
	}
	major := header[4]
	if major != SupportedMajor {
		return nil, &ProtocolError{Kind: ErrUnsupportedMajor, Msg: fmt.Sprintf("major version %d not supported (supported: %d)", major, SupportedMajor)}
	}
	minor := header[5]
	sessionID := binary.BigEndian.Uint64(header[6:14])
	sequence := binary.BigEndian.Uint64(header[14:22])
	mt := MessageType(binary.BigEndian.Uint16(header[22:24]))
	payloadLen := binary.BigEndian.Uint32(header[24:28])
	var digest [32]byte
	copy(digest[:], header[28:60])

	if int(payloadLen) > maxFrame-HeaderSize {
		return nil, &ProtocolError{Kind: ErrPayloadTooLarge, Msg: fmt.Sprintf("payload length %d exceeds max %d", payloadLen, maxFrame-HeaderSize)}
	}
	if mt != 0 && !mt.Known() {
		return nil, &ProtocolError{Kind: ErrUnknownMessageType, Msg: fmt.Sprintf("message type %d", mt)}
	}
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}
	if got := digestOf(payload); got != digest {
		return nil, &ProtocolError{Kind: ErrDigestMismatch, Msg: "payload digest does not match header digest"}
	}
	if len(payload) > 0 && !json.Valid(payload) {
		return nil, &ProtocolError{Kind: ErrInvalidJSON, Msg: "payload is not valid JSON"}
	}
	return &Frame{
		Major:       major,
		Minor:       minor,
		SessionID:   sessionID,
		Sequence:    sequence,
		MessageType: mt,
		Payload:     payload,
	}, nil
}

// Decoder is an incremental frame decoder. Bytes are fed via Feed; complete,
// validated frames are delivered to the supplied callback. The decoder buffers
// partial frames across Feed calls and drains multiple frames from a single
// call, so it handles both split and sticky byte streams. On any validation
// failure Feed returns a *ProtocolError and the buffer is reset so the caller
// can decide how to resync; the failed frame is never delivered.
type Decoder struct {
	maxFrame int
	buf      []byte
	onFrame  func(Frame) error
}

// NewDecoder creates a decoder that delivers frames to onFrame. maxFrame caps
// the total size of a single frame (header + payload); pass 0 for the default.
func NewDecoder(maxFrame int, onFrame func(Frame) error) *Decoder {
	if maxFrame <= 0 {
		maxFrame = DefaultMaxFrame
	}
	return &Decoder{maxFrame: maxFrame, onFrame: onFrame}
}

// Feed appends bytes and decodes as many complete frames as available. It
// returns nil while waiting for more bytes (partial frame), or the first
// validation error encountered.
func (d *Decoder) Feed(p []byte) error {
	d.buf = append(d.buf, p...)
	for {
		if len(d.buf) < HeaderSize {
			return nil
		}
		// Peek header to learn the payload length. Do not consume yet.
		if d.buf[0] != Magic[0] || d.buf[1] != Magic[1] || d.buf[2] != Magic[2] || d.buf[3] != Magic[3] {
			d.buf = d.buf[:0]
			return &ProtocolError{Kind: ErrBadMagic, Msg: "magic bytes do not match"}
		}
		major := d.buf[4]
		if major != SupportedMajor {
			d.buf = d.buf[:0]
			return &ProtocolError{Kind: ErrUnsupportedMajor, Msg: fmt.Sprintf("major version %d not supported (supported: %d)", major, SupportedMajor)}
		}
		mt := MessageType(binary.BigEndian.Uint16(d.buf[22:24]))
		if mt != 0 && !mt.Known() {
			d.buf = d.buf[:0]
			return &ProtocolError{Kind: ErrUnknownMessageType, Msg: fmt.Sprintf("message type %d", mt)}
		}
		payloadLen := int(binary.BigEndian.Uint32(d.buf[24:28]))
		if payloadLen > d.maxFrame-HeaderSize {
			d.buf = d.buf[:0]
			return &ProtocolError{Kind: ErrPayloadTooLarge, Msg: fmt.Sprintf("payload length %d exceeds max %d", payloadLen, d.maxFrame-HeaderSize)}
		}
		total := HeaderSize + payloadLen
		if len(d.buf) < total {
			// Partial frame: wait for more bytes.
			return nil
		}
		header := d.buf[:HeaderSize]
		payload := d.buf[HeaderSize:total]
		var digest [32]byte
		copy(digest[:], header[28:60])
		if got := digestOf(payload); got != digest {
			d.buf = d.buf[:0]
			return &ProtocolError{Kind: ErrDigestMismatch, Msg: "payload digest does not match header digest"}
		}
		if len(payload) > 0 && !json.Valid(payload) {
			d.buf = d.buf[:0]
			return &ProtocolError{Kind: ErrInvalidJSON, Msg: "payload is not valid JSON"}
		}
		frame := Frame{
			Major:       header[4],
			Minor:       header[5],
			SessionID:   binary.BigEndian.Uint64(header[6:14]),
			Sequence:    binary.BigEndian.Uint64(header[14:22]),
			MessageType: mt,
			Payload:     append([]byte(nil), payload...),
		}
		// Consume the frame bytes.
		d.buf = d.buf[total:]
		if err := d.onFrame(frame); err != nil {
			// The business layer rejected delivery; surface its error and stop
			// draining further frames this call.
			return err
		}
	}
}

// Buffered returns the number of undecoded bytes currently held.
func (d *Decoder) Buffered() int { return len(d.buf) }

// Reset discards any buffered bytes.
func (d *Decoder) Reset() { d.buf = d.buf[:0] }
