// Package transport provides the in-memory, synchronous byte-stream abstraction
// used to connect the control service's per-node sessions to virtual nodes in
// tests without any real networking.
//
// A Wire is non-blocking: ReadChunk returns whatever bytes the peer has written
// so far (or nil if none), so a test can deterministically pump both ends to
// quiescence. NewDuplex returns a connected pair: bytes written to one
// Endpoint appear on the other's ReadChunk.
package transport

import (
	"errors"
	"io"
	"sync"
)

// Wire is the bidirectional byte channel a session and a virtual node share.
// It is safe for concurrent use.
type Wire interface {
	io.Closer
	// ReadChunk returns bytes currently buffered from the peer without blocking.
	// It returns (nil, nil) when nothing is available and (nil, io.EOF) once the
	// peer has closed its side.
	ReadChunk() ([]byte, error)
	// Write sends bytes to the peer.
	Write(p []byte) (int, error)
}

// Endpoint is one side of an in-memory duplex byte stream.
type Endpoint struct {
	mu     sync.Mutex
	buf    []byte
	peer   *Endpoint
	closed bool
	// readClosed is set when the peer closes, so ReadChunk can return io.EOF.
	readClosed bool
}

// NewDuplex returns two connected endpoints. Bytes written to a appear on b's
// ReadChunk, and vice versa.
func NewDuplex() (a, b *Endpoint) {
	a = &Endpoint{}
	b = &Endpoint{}
	a.peer = b
	b.peer = a
	return a, b
}

// Write appends bytes to the peer's read buffer.
func (e *Endpoint) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	e.peer.mu.Lock()
	defer e.peer.mu.Unlock()
	if e.peer.closed {
		return 0, io.ErrClosedPipe
	}
	e.peer.buf = append(e.peer.buf, p...)
	return len(p), nil
}

// ReadChunk drains and returns the bytes the peer has written. It is
// non-blocking: it returns (nil, nil) if no bytes are available, and
// (nil, io.EOF) if the peer has closed and no bytes remain.
func (e *Endpoint) ReadChunk() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.buf) > 0 {
		out := e.buf
		e.buf = nil
		return out, nil
	}
	if e.readClosed {
		return nil, io.EOF
	}
	return nil, nil
}

// Close marks this side as closed. The peer will see io.EOF from ReadChunk once
// it has drained any remaining buffered bytes.
func (e *Endpoint) Close() error {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	// Signal the peer that no more writes are coming.
	if e.peer != nil {
		e.peer.mu.Lock()
		e.peer.readClosed = true
		e.peer.mu.Unlock()
	}
	return nil
}

// ErrWouldBlock is retained for callers that prefer a sentinel over (nil,nil).
var ErrWouldBlock = errors.New("transport: would block")
