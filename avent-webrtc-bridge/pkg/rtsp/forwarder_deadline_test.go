package rtsp

import (
	"errors"
	"net"
	"testing"
	"time"
)

type deadlineConn struct {
	net.Conn
	deadline time.Time
	writeErr error
	closed   bool
}

func (c *deadlineConn) SetWriteDeadline(deadline time.Time) error {
	c.deadline = deadline
	return nil
}

func (c *deadlineConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(p), nil
}

func (c *deadlineConn) Close() error { c.closed = true; return nil }

// TestInterleavedWriteClearsDeadline protects later RTSP control responses.
func TestInterleavedWriteClearsDeadline(t *testing.T) {
	for _, writeErr := range []error{nil, errors.New("write failed")} {
		conn := &deadlineConn{writeErr: writeErr}
		forwarder := NewRTPForwarder()
		if err := forwarder.sendInterleavedRTP(conn, 0, []byte{1}); !errors.Is(err, writeErr) {
			t.Fatalf("got %v, want %v", err, writeErr)
		}
		if !conn.deadline.IsZero() {
			t.Fatal("media write left a deadline on the shared RTSP connection")
		}
		if conn.closed != (writeErr != nil) {
			t.Fatal("failed media writes must still close the consumer")
		}
	}
}
