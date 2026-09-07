package rtsp

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"avent-webrtc-bridge/pkg/tuya"
	pion "github.com/pion/webrtc/v4"
)

// TestStopCancelsBridgeDiscovery interrupts each HTTP phase of real bridge startup.
func TestStopCancelsBridgeDiscovery(t *testing.T) {
	for _, action := range []string{
		"smartlife.m.p2p.main.pre.link.get",
		"smartlife.m.rtc.config.get",
		"smartlife.m.rtc.session.init",
		"smartlife.m.user.info.get",
	} {
		t.Run(action, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := r.ParseForm(); err != nil {
					t.Error(err)
					return
				}
				if r.Form.Get("a") == action {
					close(entered)
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				_, _ = w.Write([]byte(`{"success":true,"result":{}}`))
			}))
			defer server.Close()
			defer close(release)
			bridge := newPartialBridge(t)
			t.Cleanup(bridge.Stop)
			bridge.SetMobileClient(&tuya.MobileSDKClient{BaseURL: server.URL})
			started := make(chan error, 1)
			go func() { started <- bridge.Start() }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("startup did not reach the stalled HTTP phase")
			}
			stopped := make(chan struct{})
			go func() { bridge.Stop(); close(stopped) }()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("Stop blocked behind an uncancelled startup HTTP request")
			}
			select {
			case err := <-started:
				if err == nil {
					t.Fatal("canceled startup succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("Start did not return after Stop")
			}
			if bridge.peerConnection.ConnectionState() != pion.PeerConnectionStateClosed || bridge.ctx.Err() == nil {
				t.Fatal("canceled discovery retained peer resources")
			}
		})
	}
}
