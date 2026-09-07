package rtsp

import (
	"sync"
	"testing"
)

// TestRegistrationUsesReplacementBridge checks that lookup and registration are atomic.
func TestRegistrationUsesReplacementBridge(t *testing.T) {
	_, stream := newTestCameraStream(t)
	stream.connecting = true
	defer stream.Stop()
	client := &RTSPClient{session: "viewer"}
	stream.clients[client.session] = client
	old := stream.Bridge()
	stream.mutex.Lock()
	stream.replaceBridge()
	stream.mutex.Unlock()
	err := stream.withForwarder(client, func(forwarder *RTPForwarder) error {
		if forwarder == old.rtpForwarder {
			t.Fatal("registration selected the discarded bridge")
		}
		return forwarder.AddUDPClient(client.session, "127.0.0.2", 5000, 5002)
	})
	if err != nil || old.rtpForwarder.GetClientCount() != 0 || stream.Bridge().rtpForwarder.GetClientCount() != 1 {
		t.Fatalf("incorrect replacement registration: %v", err)
	}
	stream.RemoveClient(client.session)
	if err := stream.withForwarder(client, func(*RTPForwarder) error {
		t.Fatal("removed viewer was allowed to allocate sockets")
		return nil
	}); err == nil {
		t.Fatal("removed viewer registration succeeded")
	}
}

// TestConcurrentRegistrationAndReplacement leaves no sockets in discarded forwarders.
func TestConcurrentRegistrationAndReplacement(t *testing.T) {
	_, stream := newTestCameraStream(t)
	stream.connecting = true
	defer stream.Stop()
	client := &RTSPClient{session: "viewer"}
	stream.clients[client.session] = client
	var retired []*RTPForwarder
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			stream.mutex.Lock()
			retired = append(retired, stream.webrtcBridge.rtpForwarder)
			stream.replaceBridge()
			stream.mutex.Unlock()
		}
	})
	wg.Go(func() {
		for range 100 {
			if err := stream.withForwarder(client, func(forwarder *RTPForwarder) error {
				return forwarder.AddUDPClient(client.session, "127.0.0.2", 5000, 5002)
			}); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()
	for _, forwarder := range retired {
		if forwarder.GetClientCount() != 0 {
			t.Fatal("discarded forwarder retained a registered viewer")
		}
	}
}

// TestStoppedForwarderRejectsRegistration prevents late startup callbacks leaking sockets.
func TestStoppedForwarderRejectsRegistration(t *testing.T) {
	forwarder := NewRTPForwarder()
	forwarder.Stop()
	if err := forwarder.AddUDPClient("late", "127.0.0.2", 5000, 5002); err == nil {
		t.Fatal("stopped forwarder accepted UDP registration")
	}
	if err := forwarder.AddTCPClient("late", nil, 0, 2, 4); err == nil {
		t.Fatal("stopped forwarder accepted TCP registration")
	}
	if _, err := forwarder.SetupUDPBackchannel("late", 5000); err == nil {
		t.Fatal("stopped forwarder accepted backchannel registration")
	}
	if forwarder.GetClientCount() != 0 {
		t.Fatal("stopped forwarder allocated client resources")
	}
}
