package rtsp

import (
	"net"
	"testing"
	"time"

	"github.com/pion/rtp"
)

// TestUDPVideoAndAudioUseRTSPPeerAddress verifies initial and subsequent SETUP destinations.
func TestUDPVideoAndAudioUseRTSPPeerAddress(t *testing.T) {
	// A non-localhost loopback address detects the original localhost bug,
	// without contacting a camera or requiring external network access.
	peer := net.ParseIP("127.0.0.2")
	video, err := net.ListenUDP("udp4", &net.UDPAddr{IP: peer})
	if err != nil {
		t.Fatal(err)
	}
	defer video.Close()
	audio, err := net.ListenUDP("udp4", &net.UDPAddr{IP: peer})
	if err != nil {
		t.Fatal(err)
	}
	defer audio.Close()
	forwarder := NewRTPForwarder()
	defer forwarder.Stop()
	// Exercise initial SETUP and the subsequent audio SETUP update.
	if err := forwarder.AddUDPClient("peer", peer.String(), video.LocalAddr().(*net.UDPAddr).Port, 0); err != nil {
		t.Fatal(err)
	}
	if err := forwarder.AddUDPClient("peer", peer.String(), video.LocalAddr().(*net.UDPAddr).Port, audio.LocalAddr().(*net.UDPAddr).Port); err != nil {
		t.Fatal(err)
	}
	forwarder.ForwardVideoPacket(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96}, Payload: []byte{1, 2, 3}})
	forwarder.ForwardAudioPacket(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 0}, Payload: []byte{4, 5, 6}})
	for _, receiver := range []*net.UDPConn{video, audio} {
		_ = receiver.SetReadDeadline(time.Now().Add(time.Second))
		buffer := make([]byte, 1500)
		if _, _, err := receiver.ReadFromUDP(buffer); err != nil {
			t.Fatalf("RTP did not reach actual peer: %v", err)
		}
	}
}

// TestUDPRejectsNonIPDestination rejects hostnames instead of trusting arbitrary destinations.
func TestUDPRejectsNonIPDestination(t *testing.T) {
	forwarder := NewRTPForwarder()
	defer forwarder.Stop()
	if err := forwarder.AddUDPClient("peer", "arbitrary.example", 5000, 5002); err == nil {
		t.Fatal("only the actual RTSP peer IP may be used as a destination")
	}
}
