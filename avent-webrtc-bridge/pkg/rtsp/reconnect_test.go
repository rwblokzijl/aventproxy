package rtsp

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"avent-webrtc-bridge/pkg/storage"
)

// Exercise real RTSP clients and startup retries, with an isolated camera whose
// missing session deliberately fails before any cloud or physical-camera call.
func TestRTSPFailedStartsCloseClientsAndAllowReconnect(t *testing.T) {
	t.Chdir(t.TempDir())
	manager, err := storage.NewStorageManager()
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SaveUser("test", "test@example.invalid", nil); err != nil {
		t.Fatal(err)
	}
	users, err := manager.ListUsers()
	if err != nil || len(users) != 1 {
		t.Fatalf("fixture users: %v", err)
	}
	if err := manager.UpdateCamerasForUser(users[0].UserKey, []storage.CameraInfo{{
		UserKey: users[0].UserKey, DeviceID: "test", DeviceName: "Test camera", RTSPPath: "/test",
	}}); err != nil {
		t.Fatal(err)
	}
	server := NewRTSPServer(0, manager)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	address := server.listener.Addr().(*net.TCPAddr)
	for cycle := range 3 {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", address.Port), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(6 * time.Second))
		if _, err := fmt.Fprint(conn, "OPTIONS rtsp://localhost/test RTSP/1.0\r\nCSeq: 1\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(conn)
		line, err := reader.ReadString('\n')
		if err != nil || !strings.Contains(line, "200 OK") {
			t.Fatalf("cycle %d: server unresponsive: %q %v", cycle, line, err)
		}
		_, err = io.Copy(io.Discard, reader)
		_ = conn.Close()
		if err != nil {
			t.Fatalf("cycle %d: failed stream did not close consumer: %v", cycle, err)
		}
		deadline := time.Now().Add(time.Second)
		for server.GetStats().ClientCount != 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if stats := server.GetStats(); stats.ClientCount != 0 {
			t.Fatalf("cycle %d: leaked consumers: %+v", cycle, stats)
		}
	}
}
