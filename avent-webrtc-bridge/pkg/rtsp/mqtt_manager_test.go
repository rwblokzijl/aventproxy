package rtsp

import (
	"context"
	"errors"
	"testing"
	"time"

	"avent-webrtc-bridge/pkg/tuya"
)

// awaitMQTTResult fails promptly when a canceled manager still blocks a caller.
func awaitMQTTResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("MQTT operation did not unblock promptly")
		return nil
	}
}

// TestMQTTManagerStopCancelsConnections exercises the real RTSP Stop ownership path.
func TestMQTTManagerStopCancelsConnections(t *testing.T) {
	server := NewRTSPServer(0, nil)
	server.running = true
	manager := NewMQTTManager(server.ctx, nil)
	server.mqttManager = manager
	defer manager.Stop()
	entered := make(chan struct{})
	manager.connectFn = func(ctx context.Context, _ string) (*tuya.MQTTClient, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := manager.GetClient(t.Context(), "camera"); first <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("connection attempt did not start")
	}
	go func() { _, err := manager.GetClient(t.Context(), "camera"); second <- err }()
	stopped := make(chan error, 1)
	go func() { stopped <- server.Stop() }()
	if err := awaitMQTTResult(t, stopped); err != nil {
		t.Fatal(err)
	}
	for _, result := range []<-chan error{first, second} {
		if err := awaitMQTTResult(t, result); !errors.Is(err, context.Canceled) {
			t.Fatalf("want canceled caller, got %v", err)
		}
	}
	if len(manager.clients) != 0 || len(manager.pending) != 0 {
		t.Fatal("stopped manager retained sessions or pending attempts")
	}
}

// TestMQTTManagerSlowDeviceDoesNotBlockOthers protects parallel device acquisition.
func TestMQTTManagerSlowDeviceDoesNotBlockOthers(t *testing.T) {
	manager := NewMQTTManager(t.Context(), nil)
	defer manager.Stop()
	entered := make(chan struct{})
	want := errors.New("independent connection failure")
	manager.connectFn = func(ctx context.Context, device string) (*tuya.MQTTClient, error) {
		if device == "slow" {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, want
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	slow, other := make(chan error, 1), make(chan error, 1)
	go func() { _, err := manager.GetClient(ctx, "slow"); slow <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow connection did not start")
	}
	go func() { _, err := manager.GetClient(t.Context(), "other"); other <- err }()
	if err := awaitMQTTResult(t, other); !errors.Is(err, want) {
		t.Fatal(err)
	}
	cancel()
	if err := awaitMQTTResult(t, slow); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// TestMQTTManagerDiscardsLateConnection rejects results after invalidation or Stop.
func TestMQTTManagerDiscardsLateConnection(t *testing.T) {
	for _, stop := range []bool{false, true} {
		manager := NewMQTTManager(t.Context(), nil)
		defer manager.Stop()
		entered, release := make(chan struct{}), make(chan struct{})
		manager.connectFn = func(ctx context.Context, _ string) (*tuya.MQTTClient, error) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return &tuya.MQTTClient{}, nil
		}
		result := make(chan error, 1)
		go func() { _, err := manager.GetClient(t.Context(), "camera"); result <- err }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("connection did not start")
		}
		if stop {
			manager.Stop()
		} else {
			manager.InvalidateClient("camera")
		}
		close(release)
		if err := awaitMQTTResult(t, result); !errors.Is(err, context.Canceled) {
			t.Fatalf("late result was accepted: %v", err)
		}
		if len(manager.clients) != 0 {
			t.Fatal("late connection was cached")
		}
	}
}

// TestMQTTBackoffCancellation bounds even a long retry delay by caller cancellation.
func TestMQTTBackoffCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- waitMQTTBackoff(ctx, time.Hour) }()
	cancel()
	if err := awaitMQTTResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
