package tuya

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestMQTTConnectTokenCancellation uses a broker that never acknowledges CONNECT.
func TestMQTTConnectTokenCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		client, err := NewMobileMqttClientContext(ctx, &MobileMQTTConfig{
			BrokerURL: "tcp://" + listener.Addr().String(), ClientID: "test", UID: "test",
		})
		if client != nil {
			client.Stop()
		}
		result <- err
	}()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(time.Second):
		t.Fatal("MQTT did not connect to the isolated broker")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("constructor ignored cancellation during CONNECT wait")
	}
}

// TestMQTTDiscoveryCancellation cancels broker discovery before a stalled response.
func TestMQTTDiscoveryCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	client := &MobileSDKClient{BaseURL: server.URL}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.GetUserInfoContext(ctx); result <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("discovery request did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cloud discovery ignored cancellation")
	}
}
