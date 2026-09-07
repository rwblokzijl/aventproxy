package tuya

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type stalledToken struct{}

// Wait detects accidental unbounded waiting in the acknowledgement helper.
func (stalledToken) Wait() bool { panic("unbounded MQTT wait") }

// WaitTimeout models a broker that never acknowledges a token.
func (stalledToken) WaitTimeout(d time.Duration) bool { time.Sleep(d); return false }

// Done never signals completion for the stalled-broker fixture.
func (stalledToken) Done() <-chan struct{} { return nil }

// Error has no terminal error because the fixture never completes.
func (stalledToken) Error() error { return nil }

// TestMQTTAcknowledgementHasDeadline rejects missing acknowledgements after a bounded wait.
func TestMQTTAcknowledgementHasDeadline(t *testing.T) {
	if err := waitMQTTToken(stalledToken{}, time.Millisecond); err == nil {
		t.Fatal("missing acknowledgement was treated as success")
	}
}

// TestMQTTSubscriptionWaitCanBeCanceled propagates caller cancellation through readiness.
func TestMQTTSubscriptionWaitCanBeCanceled(t *testing.T) {
	client := &MQTTClient{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := client.WaitConnected(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// TestConcurrentMQTTSessionCleanup checks session-map synchronization during teardown.
func TestConcurrentMQTTSessionCleanup(t *testing.T) {
	client := &MQTTClient{}
	var wg sync.WaitGroup
	for worker := range 20 {
		wg.Go(func() {
			for i := range 100 {
				id := fmt.Sprintf("%d-%d", worker, i)
				client.AddCameraClient(id, &MQTTCameraClient{})
				client.RemoveCameraClient(id)
				_ = client.IsConnected()
			}
		})
	}
	wg.Go(client.Stop)
	wg.Wait()
	if len(client.cameras) != 0 {
		t.Fatal("session registrations leaked")
	}
	if err := (&MQTTCameraClient{mqttClient: client}).SendDisconnect(); err == nil {
		t.Fatal("disconnected client must fail fast")
	}
}
