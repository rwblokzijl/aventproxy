package rtsp

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"avent-webrtc-bridge/pkg/storage"
	"avent-webrtc-bridge/pkg/tuya"
	pion "github.com/pion/webrtc/v4"
)

func newPartialBridge(t *testing.T) *WebRTCBridge {
	t.Helper()
	bridge := NewWebRTCBridge(&storage.CameraInfo{DeviceName: "Test camera"}, "hd", nil, nil)
	if err := bridge.setupPeerConnection(&tuya.WebRTCConfig{}); err != nil {
		t.Fatal(err)
	}
	// Allocate real ICE sockets, as a failed camera negotiation does.
	if _, err := bridge.peerConnection.CreateDataChannel("test", nil); err != nil {
		t.Fatal(err)
	}
	offer, err := bridge.peerConnection.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gathered := pion.GatheringCompletePromise(bridge.peerConnection)
	if err := bridge.peerConnection.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gathered:
	case <-time.After(5 * time.Second):
		t.Fatal("local ICE gathering timed out")
	}
	t.Cleanup(func() { _ = bridge.peerConnection.Close() })
	return bridge
}

func TestStopClosesNeverConnectedPeer(t *testing.T) {
	bridge := newPartialBridge(t)
	bridge.Stop()
	if bridge.peerConnection.ConnectionState() != pion.PeerConnectionStateClosed {
		t.Error("failed negotiation leaked its peer connection")
	}
	if !errors.Is(bridge.ctx.Err(), context.Canceled) {
		t.Error("failed negotiation did not cancel its workers")
	}
	bridge.Stop() // Idempotent, even after a partial start.
}

func TestRepeatedFailedPeersDoNotLeakDescriptors(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux descriptor accounting")
	}
	count := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	baseline := count()
	for range 40 {
		bridge := newPartialBridge(t)
		bridge.Stop()
	}
	deadline := time.Now().Add(3 * time.Second)
	for count() > baseline+4 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := count(); got > baseline+4 {
		t.Fatalf("descriptor leak after 40 failed peers: before=%d after=%d", baseline, got)
	}
}

func TestConcurrentStopIsSafe(t *testing.T) {
	bridge := newPartialBridge(t)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(bridge.Stop)
	}
	wg.Wait()
	if bridge.peerConnection.ConnectionState() != pion.PeerConnectionStateClosed {
		t.Fatal("concurrent stop left peer open")
	}
}

func TestFailedStartCleansAllocatedResources(t *testing.T) {
	bridge := newPartialBridge(t)
	// Missing login state makes the real Start error path fail before an offer.
	if err := bridge.Start(); err == nil {
		t.Fatal("expected missing login state to fail")
	}
	if bridge.peerConnection.ConnectionState() != pion.PeerConnectionStateClosed {
		t.Fatal("Start error did not clean up allocated peer")
	}
	if err := bridge.Start(); err == nil {
		t.Fatal("a failed bridge must not be restarted in place")
	}
}

func TestDisconnectThenStopClosesPeer(t *testing.T) {
	bridge := newPartialBridge(t)
	bridge.connected.Store(true)
	bridge.handleError(errors.New("camera disconnected"))
	bridge.Stop()
	if bridge.IsConnected() || bridge.peerConnection.ConnectionState() != pion.PeerConnectionStateClosed {
		t.Fatal("disconnect before Stop leaked an established peer")
	}
}

func TestErrorCompletesNegotiationWithoutCallback(t *testing.T) {
	bridge := NewWebRTCBridge(&storage.CameraInfo{}, "hd", nil, nil)
	want := errors.New("camera disconnected")
	bridge.handleError(want)
	if got := bridge.waitForConnection(time.Second); !errors.Is(got, want) {
		t.Fatalf("want original error, got %v", got)
	}
}

func TestErrorCallbackDoesNotBlockSignaling(t *testing.T) {
	bridge := NewWebRTCBridge(&storage.CameraInfo{}, "hd", nil, nil)
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	bridge.SetErrorHandler(func(error) { close(entered); <-release })
	go func() { bridge.handleError(errors.New("disconnect")); close(returned) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("error callback was not invoked")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("signaling callback blocked on stream teardown")
	}
}

func TestNegotiationTimeoutAndCancellation(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		bridge := NewWebRTCBridge(&storage.CameraInfo{}, "hd", nil, nil)
		defer bridge.Stop()
		if err := bridge.waitForConnection(10 * time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want deadline exceeded, got %v", err)
		}
	})
	t.Run("stop wakes negotiating start", func(t *testing.T) {
		bridge := NewWebRTCBridge(&storage.CameraInfo{}, "hd", nil, nil)
		bridge.mutex.Lock() // The resource lock held by Start during negotiation.
		stopped := make(chan struct{})
		go func() { bridge.Stop(); close(stopped) }()
		err := bridge.waitForConnection(time.Second)
		bridge.mutex.Unlock()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stop did not cancel negotiation: %v", err)
		}
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("Stop deadlocked behind Start")
		}
	})
}

func TestStaleBridgeErrorCannotStopReplacement(t *testing.T) {
	_, stream := newTestCameraStream(t)
	old := stream.webrtcBridge
	stream.mutex.Lock()
	stream.replaceBridge()
	stream.active = true
	stream.mutex.Unlock()
	stream.handleBridgeErrorFrom(old, errors.New("late old peer callback"))
	if !stream.active || stream.webrtcBridge.ctx.Err() != nil {
		t.Fatal("stale callback stopped replacement")
	}
	stream.Stop()
}

func TestNegotiatingBridgeErrorLeavesRetryOwnerInControl(t *testing.T) {
	_, stream := newTestCameraStream(t)
	stream.connecting = true
	stream.handleBridgeErrorFrom(stream.webrtcBridge, errors.New("camera offline"))
	if !stream.connecting {
		t.Fatal("async callback stole startup retry ownership")
	}
	stream.Stop()
}
