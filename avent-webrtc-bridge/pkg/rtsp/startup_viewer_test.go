package rtsp

import "testing"

// TestStartupWithoutViewersStopsBridge exercises the post-negotiation transition.
func TestStartupWithoutViewersStopsBridge(t *testing.T) {
	server, stream := newTestCameraStream(t)
	stream.connecting = true
	stream.webrtcBridge.connected.Store(true)
	stream.clients["viewer"] = &RTSPClient{session: "viewer"}
	stream.RemoveClient("viewer")
	stream.mutex.Lock()
	activated := stream.activateStreamLocked()
	stream.mutex.Unlock()
	if activated || stream.active || stream.connecting || stream.webrtcBridge.IsConnected() {
		t.Fatal("startup retained an active bridge without viewers")
	}
	if stream.webrtcBridge.ctx.Err() == nil {
		t.Fatal("orphan startup did not tear down the bridge")
	}
	waitStreamRemoved(t, server, stream.streamId)
}

// TestStartupWithViewersActivatesBridge preserves normal successful startup.
func TestStartupWithViewersActivatesBridge(t *testing.T) {
	_, stream := newTestCameraStream(t)
	defer stream.Stop()
	stream.connecting = true
	stream.webrtcBridge.connected.Store(true)
	stream.clients["viewer"] = &RTSPClient{session: "viewer"}
	stream.mutex.Lock()
	activated := stream.activateStreamLocked()
	stream.mutex.Unlock()
	if !activated || !stream.active || stream.connecting || !stream.webrtcBridge.IsConnected() {
		t.Fatal("startup failed to activate a bridge with a viewer")
	}
}
