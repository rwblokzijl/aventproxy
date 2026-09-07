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

func (stalledToken) Wait() bool                       { panic("unbounded MQTT wait") }
func (stalledToken) WaitTimeout(d time.Duration) bool { time.Sleep(d); return false }
func (stalledToken) Done() <-chan struct{}            { return nil }
func (stalledToken) Error() error                     { return nil }

func TestMQTTAcknowledgementHasDeadline(t *testing.T) {
	if err := waitMQTTToken(stalledToken{}, time.Millisecond); err == nil {
		t.Fatal("missing acknowledgement was treated as success")
	}
}

func TestMQTTSubscriptionWaitCanBeCanceled(t *testing.T) {
	client := &MQTTClient{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := client.WaitConnected(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

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
