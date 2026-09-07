package rtsp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pion "github.com/pion/webrtc/v4"
	"golang.org/x/net/publicsuffix"

	"avent-webrtc-bridge/pkg/core"
	"avent-webrtc-bridge/pkg/storage"
	"avent-webrtc-bridge/pkg/tuya"
	"avent-webrtc-bridge/pkg/utils"
	"avent-webrtc-bridge/pkg/webrtc"

	"github.com/pion/rtp"
)

type WebRTCBridge struct {
	camera         *storage.CameraInfo
	resolution     string
	streamType     int
	isHEVC         bool
	user           *storage.UserSession
	storageManager *storage.StorageManager
	mobileClient   *tuya.MobileSDKClient

	// WebRTC components
	peerConnection *pion.PeerConnection
	dataChannel    *pion.DataChannel
	mqttClient     *tuya.MQTTClient
	cameraClient   *tuya.MQTTCameraClient
	rtpForwarder   *RTPForwarder

	// State
	connected         atomic.Bool
	stopped           bool
	callbackMutex     sync.RWMutex
	signalingMutex    sync.Mutex
	ownsClient        bool
	answerReceived    bool
	pendingCandidates []pion.ICECandidateInit
	waiter            utils.Waiter
	mutex             sync.RWMutex

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// RTP forwarding
	videoTrack  *pion.TrackRemote
	audioTrack  *pion.TrackRemote
	backchannel *pion.TrackLocalStaticRTP

	// Talkback offers the camera two-way audio. Off by default: an offer with
	// a sendrecv audio direction makes the camera set DPS 253 (app_talking),
	// take the speaker and stop whatever lullaby is playing, then restart it
	// with a fresh timer when the stream closes (issue #72). Watching must not
	// disturb the room, so the caller opts in.
	Talkback bool

	// Callbacks
	OnVideoPacket func(packet *rtp.Packet)
	OnAudioPacket func(packet *rtp.Packet)
	OnError       func(error)
}

// NewWebRTCBridge allocates a single-use camera session without opening connections.
func NewWebRTCBridge(camera *storage.CameraInfo, streamResolution string, user *storage.UserSession, storageManager *storage.StorageManager) *WebRTCBridge {
	ctx, cancel := context.WithCancel(context.Background())

	wb := &WebRTCBridge{
		camera:         camera,
		resolution:     streamResolution,
		user:           user,
		rtpForwarder:   NewRTPForwarder(),
		storageManager: storageManager,
		waiter:         utils.Waiter{},
		ctx:            ctx,
		cancel:         cancel,
	}

	wb.rtpForwarder.OnBackchannelAudio = wb.ForwardBackchannelAudioPacket

	return wb
}

func (wb *WebRTCBridge) SetMobileClient(client *tuya.MobileSDKClient) {
	wb.mobileClient = client
}

// SetMQTTClient supplies a shared MQTT session that bridge teardown must not close.
func (wb *WebRTCBridge) SetMQTTClient(client *tuya.MQTTClient) {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	wb.mqttClient = client
}

// Start negotiates media with bounded waits and cleans every partially failed session.
func (wb *WebRTCBridge) Start() (err error) {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	if wb.connected.Load() {
		return errors.New("bridge already connected")
	}
	if wb.stopped || wb.ctx.Err() != nil {
		return errors.New("bridge already stopped; create a new bridge")
	}
	// Every partially allocated session owns sockets, even if it never connects.
	defer func() {
		if err != nil {
			wb.stopLocked()
		}
	}()

	core.Logger.Info().Msgf("Starting WebRTC bridge for camera: %s", wb.camera.DeviceName)

	var webRTCConfig *tuya.WebRTCConfigResponse
	var mobileMqttsUrl string

	if wb.mobileClient != nil {
		// Mobile SDK path
		core.Logger.Info().Msg("Using Tuya Mobile SDK API")

		if err := wb.mobileClient.P2PPreLink(wb.camera.DeviceID); err != nil {
			core.Logger.Warn().Err(err).Msgf(
				"P2P pre-link failed for camera %s (non-fatal, but the WebRTC config may be refused)",
				wb.camera.DeviceName,
			)
		}

		webRTCConfig, err = wb.mobileClient.GetWebRTCConfig(wb.camera.DeviceID)
		if err != nil {
			return fmt.Errorf(
				"failed to get WebRTC config for camera %s (id %s): %v",
				wb.camera.DeviceName, wb.camera.DeviceID, err,
			)
		}

		if err := wb.mobileClient.RTCSessionInit(wb.camera.DeviceID); err != nil {
			core.Logger.Warn().Err(err).Msg("RTC session init failed (non-fatal)")
		}

		if wb.mqttClient == nil {
			// No pre-set MQTT client — create one (fallback for non-managed mode)
			userInfo, err := wb.mobileClient.GetUserInfo()
			if err != nil {
				return fmt.Errorf("failed to get user info: %v", err)
			}
			mobileMqttsUrl = userInfo.Domain.MobileMqttsUrl

			ecode := wb.mobileClient.Ecode
			uid := wb.mobileClient.UID
			mqttDerived := wb.mobileClient.DeriveMQTTConfig(ecode)
			mqttUsername := wb.mobileClient.DeriveMQTTUsername(wb.mobileClient.SID, ecode, wb.mobileClient.PartnerIdentity)
			mqttClientID := wb.mobileClient.DeriveMQTTClientID(uid)
			subscribeTopic := fmt.Sprintf("/av/u/%s", uid)

			wb.mqttClient, err = tuya.NewMobileMqttClientContext(wb.ctx, &tuya.MobileMQTTConfig{
				Username:       mqttUsername,
				Password:       mqttDerived.Password,
				ClientID:       mqttClientID,
				SubscribeTopic: subscribeTopic,
				UID:            uid,
				BrokerURL:      fmt.Sprintf("ssl://%s:8883", mobileMqttsUrl),
			})
			if err != nil {
				return fmt.Errorf("failed to connect to MQTT: %v", err)
			}

			wb.ownsClient = true
			if err = wb.mqttClient.WaitConnected(wb.ctx); err != nil {
				return fmt.Errorf("MQTT connection failed: %v", err)
			}

			wb.ownsClient = true
		}

	} else {
		// Original web portal path
		httpClient := wb.createHTTPClient()
		if httpClient == nil {
			return errors.New("failed to create HTTP client")
		}

		appInfo, err := tuya.GetAppInfo(httpClient, wb.user.SessionData.ServerHost)
		if err != nil {
			return fmt.Errorf("failed to get app info: %v", err)
		}
		clientId := appInfo.Result.ClientId

		var mqttConfig *tuya.MQTTConfigResponse
		mqttConfig, err = tuya.GetMQTTConfig(httpClient, wb.user.SessionData.ServerHost)
		if err != nil {
			return fmt.Errorf("failed to get MQTT config: %v", err)
		}

		mobileMqttsUrl = wb.user.SessionData.LoginResult.Domain.MobileMqttsUrl

		webRTCConfig, err = tuya.GetWebRTCConfig(httpClient, wb.user.SessionData.ServerHost, wb.camera.DeviceID)
		if err != nil {
			return fmt.Errorf("failed to get WebRTC config: %v", err)
		}

		wb.mqttClient, err = tuya.NewMqttClientContext(wb.ctx,
			clientId,
			mobileMqttsUrl,
			&mqttConfig.Result,
		)
		if err != nil {
			return fmt.Errorf("failed to connect to MQTT: %v", err)
		}

		wb.ownsClient = true
		if err = wb.mqttClient.WaitConnected(wb.ctx); err != nil {
			return fmt.Errorf("MQTT connection failed: %v", err)
		}
	}

	// Parse skill information (may be empty for direct mode)
	var skill tuya.Skill
	if webRTCConfig.Result.Skill != "" {
		if err := json.Unmarshal([]byte(webRTCConfig.Result.Skill), &skill); err != nil {
			core.Logger.Warn().Err(err).Msg("Could not parse skill, using defaults")
		}
	}

	// Determine stream settings
	wb.streamType = tuya.GetStreamType(&skill, wb.resolution)
	wb.isHEVC = tuya.IsHEVC(&skill, wb.streamType)

	core.Logger.Info().Msgf("Stream settings - Resolution: %s, Type: %d, HEVC: %v", wb.resolution, wb.streamType, wb.isHEVC)

	// Triage aid for models that answer an offer with a disconnect instead of an
	// answer (issue #59): what the camera advertises, and what we ask it for. The
	// signaling mode is fixed at "webrtc"; Tuya's other mode is the proprietary
	// "imm" AES/KCP transport, which this bridge does not implement. No auth
	// token or credential is logged.
	core.Logger.Debug().Msgf(
		"Camera capabilities for %s: skill=%q ice_servers=%d requesting mode=webrtc stream_type=%d",
		wb.camera.DeviceName, webRTCConfig.Result.Skill,
		len(webRTCConfig.Result.P2PConfig.Ices), wb.streamType,
	)

	// Setup WebRTC peer connection
	if err := wb.setupPeerConnection(&webRTCConfig.Result); err != nil {
		return fmt.Errorf("failed to setup peer connection: %v", err)
	}

	// Setup MQTT camera client
	wb.setupMQTTCameraClient(&webRTCConfig.Result)

	// Create and send offer
	if err := wb.createAndSendOffer(); err != nil {
		return fmt.Errorf("failed to create offer: %v", err)
	}

	if err = wb.waitForConnection(20 * time.Second); err != nil {
		return fmt.Errorf("failed to establish connection: %v", err)
	}

	if err = wb.ctx.Err(); err != nil {
		return err
	}
	wb.connected.Store(true)
	core.Logger.Info().Msgf("WebRTC bridge started successfully for camera: %s", wb.camera.DeviceName)

	return nil
}

// Stop cancels negotiation and releases resources even if media never connected.
func (wb *WebRTCBridge) Stop() {
	// Wake a negotiating Start before waiting for its resource lock.
	wb.cancel()
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	wb.stopLocked()
}

// waitForConnection bounds media negotiation by both a deadline and bridge cancellation.
func (wb *WebRTCBridge) waitForConnection(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(wb.ctx, timeout)
	defer cancel()
	return wb.waiter.WaitContext(ctx)
}

// stopLocked performs idempotent resource cleanup while the bridge mutex is held.
func (wb *WebRTCBridge) stopLocked() {
	if wb.stopped {
		return
	}
	wb.stopped = true
	wb.connected.Store(false)

	core.Logger.Info().Msgf("Stopping WebRTC bridge for camera: %s", wb.camera.DeviceName)

	// Cancel context to stop all goroutines
	wb.cancel()

	// Close peer connection
	if wb.peerConnection != nil {
		wb.peerConnection.Close()
	}

	// Deregister camera client from MQTT
	if wb.mqttClient != nil && wb.cameraClient != nil {
		wb.mqttClient.RemoveCameraClient(wb.cameraClient.SessionId)
		// Best effort and time-bounded; cleanup must not wait forever for PUBACK.
		_ = wb.cameraClient.SendDisconnect()
	}

	// Only stop MQTT client if we created it (not managed by MQTTManager)
	if wb.ownsClient && wb.mqttClient != nil {
		wb.mqttClient.Stop()
	}

	// Stop RTP forwarder
	if wb.rtpForwarder != nil {
		wb.rtpForwarder.Stop()
	}

	core.Logger.Info().Msgf("WebRTC bridge stopped for camera: %s", wb.camera.DeviceName)
}

// IsConnected reports media readiness without taking the negotiation resource lock.
func (wb *WebRTCBridge) IsConnected() bool {
	return wb.connected.Load()
}

func (wb *WebRTCBridge) ForwardBackchannelAudioPacket(packet *rtp.Packet) {
	if wb.backchannel != nil {
		_ = wb.backchannel.WriteRTP(packet)
	}
}

// setupPeerConnection configures codecs, ICE and callbacks for this camera session.
func (wb *WebRTCBridge) setupPeerConnection(webRTCConfig *tuya.WebRTCConfig) error {
	// Convert ICE servers
	iceServerBytes, err := json.Marshal(webRTCConfig.P2PConfig.Ices)
	if err != nil {
		return fmt.Errorf("failed to marshal ICE servers: %v", err)
	}

	iceServers, err := webrtc.UnmarshalICEServers(iceServerBytes)
	if err != nil {
		return fmt.Errorf("failed to unmarshal ICE servers: %v", err)
	}

	// Create peer connection configuration
	conf := pion.Configuration{
		ICEServers:         iceServers,
		ICETransportPolicy: pion.ICETransportPolicyAll,
		BundlePolicy:       pion.BundlePolicyMaxBundle,
	}

	// Create WebRTC API
	api, err := webrtc.NewAPI()
	if err != nil {
		return fmt.Errorf("failed to create WebRTC API: %v", err)
	}

	// Create peer connection
	wb.peerConnection, err = api.NewPeerConnection(conf)
	if err != nil {
		return fmt.Errorf("failed to create peer connection: %v", err)
	}

	// On HEVC, use DataChannel to receive video/audio
	if wb.isHEVC {
		maxRetransmits := uint16(5)
		ordered := true

		wb.dataChannel, err = wb.peerConnection.CreateDataChannel("fmp4Stream", &pion.DataChannelInit{
			MaxRetransmits: &maxRetransmits,
			Ordered:        &ordered,
		})
		if err != nil {
			return fmt.Errorf("failed to create data channel: %w", err)
		}

		wb.dataChannel.OnMessage(func(msg pion.DataChannelMessage) {
			if msg.IsString {
				if connected, err := wb.probe(msg); err != nil {
					wb.handleError(err)
				} else if connected {
					wb.waiter.Done(nil)
				}
			} else {
				packet := &rtp.Packet{}
				if err := packet.Unmarshal(msg.Data); err != nil {
					// skip
					return
				}

				switch packet.SSRC {
				case wb.rtpForwarder.videoSSRC:
					wb.rtpForwarder.ForwardVideoPacket(packet)
				case wb.rtpForwarder.audioSSRC:
					wb.rtpForwarder.ForwardAudioPacket(packet)
				}
			}
		})

		wb.dataChannel.OnError(func(err error) {
			wb.handleError(err)
		})

		wb.dataChannel.OnClose(func() {
			wb.handleError(errors.New("datachannel: closed"))
		})

		wb.dataChannel.OnOpen(func() {
			codecRequest, _ := json.Marshal(tuya.DataChannelMessage{
				Type: "codec",
				Msg:  "",
			})

			if err := wb.sendMessageToDataChannel(codecRequest); err != nil {
				wb.handleError(fmt.Errorf("failed to send codec request: %w", err))
			}
		})
	}

	// Setup connection state handler
	wb.peerConnection.OnConnectionStateChange(func(state pion.PeerConnectionState) {
		if state == pion.PeerConnectionStateFailed || state == pion.PeerConnectionStateClosed {
			wb.handleError(errors.New("WebRTC connection failed/closed"))
		}

		if state == pion.PeerConnectionStateConnected {
			core.Logger.Info().Msgf("WebRTC connection established")

			if !wb.isHEVC && wb.ctx.Err() == nil && wb.cameraClient != nil {
				resolution := 0
				if wb.resolution == "sd" {
					resolution = 1
				}
				_ = wb.cameraClient.SendResolution(resolution)
				wb.waiter.Done(nil)
			}
		}
	})

	// Setup track handler for incoming media if not HEVC
	wb.peerConnection.OnTrack(func(track *pion.TrackRemote, receiver *pion.RTPReceiver) {
		codec := track.Codec()
		core.Logger.Trace().Msgf("Received track: %s, PayloadType: %d", codec.MimeType, codec.PayloadType)

		if track.Kind() == pion.RTPCodecTypeVideo {
			wb.videoTrack = track

			if !wb.isHEVC {
				go wb.handleVideoTrack(track)
			}
		} else if track.Kind() == pion.RTPCodecTypeAudio {
			wb.audioTrack = track

			for _, tr := range wb.peerConnection.GetTransceivers() {
				if tr.Receiver() == receiver && tr.Kind() == pion.RTPCodecTypeAudio {
					if tr.Direction() == pion.RTPTransceiverDirectionSendrecv || tr.Direction() == pion.RTPTransceiverDirectionSendonly {
						localTrack, _ := pion.NewTrackLocalStaticRTP(
							pion.RTPCodecCapability{MimeType: track.Codec().MimeType},
							"audio-backchannel", "pion",
						)
						tr.Sender().ReplaceTrack(localTrack)
						wb.backchannel = localTrack
						core.Logger.Trace().Msgf("Setup backchannel track")
						break
					}
				}
			}

			if !wb.isHEVC {
				go wb.handleAudioTrack(track)
			}
		}
	})

	return nil
}

// setupMQTTCameraClient serializes answer/candidate handling and drops canceled signaling.
func (wb *WebRTCBridge) setupMQTTCameraClient(webRTCConfig *tuya.WebRTCConfig) {
	device := &tuya.Device{
		DeviceId:   wb.camera.DeviceID,
		DeviceName: wb.camera.DeviceName,
		Category:   wb.camera.Category,
		ProductId:  wb.camera.ProductID,
		Uuid:       wb.camera.UUID,
	}

	// Create MQTT camera client
	wb.cameraClient = tuya.NewMqttCameraClient(wb.mqttClient, device, webRTCConfig)
	wb.mqttClient.AddCameraClient(wb.cameraClient.SessionId, wb.cameraClient)

	// Setup handlers
	wb.answerReceived = false
	wb.pendingCandidates = nil

	wb.cameraClient.HandleAnswer = func(answer tuya.AnswerFrame) {
		wb.signalingMutex.Lock()
		defer wb.signalingMutex.Unlock()
		if wb.ctx.Err() != nil {
			return
		}
		core.Logger.Trace().Msgf("Received WebRTC answer")

		desc := pion.SessionDescription{
			Type: pion.SDPTypePranswer,
			SDP:  answer.Sdp,
		}

		if err := wb.peerConnection.SetRemoteDescription(desc); err != nil {
			wb.handleError(err)
			return
		}

		if err := webrtc.SetAnswer(wb.peerConnection, answer.Sdp); err != nil {
			wb.handleError(err)
			return
		}

		wb.answerReceived = true

		// Flush any ICE candidates that arrived before the answer
		for _, c := range wb.pendingCandidates {
			if err := wb.peerConnection.AddICECandidate(c); err != nil {
				core.Logger.Warn().Err(err).Msg("Failed to add buffered ICE candidate")
			}
		}
		wb.pendingCandidates = nil
	}

	wb.cameraClient.HandleCandidate = func(candidate tuya.CandidateFrame) {
		wb.signalingMutex.Lock()
		defer wb.signalingMutex.Unlock()
		if wb.ctx.Err() != nil {
			return
		}
		candidateStr := strings.TrimSpace(candidate.Candidate)
		if candidateStr == "" {
			return
		}

		if strings.HasPrefix(candidateStr, "a=") {
			candidateStr = candidateStr[2:]
		}
		if !strings.HasSuffix(candidateStr, "\r\n") && !strings.HasSuffix(candidateStr, "\n") {
			candidateStr = candidateStr + "\r\n"
		}

		ice := pion.ICECandidateInit{Candidate: strings.TrimSpace(candidateStr)}

		if !wb.answerReceived {
			core.Logger.Trace().Msgf("Buffering ICE candidate (answer not yet received): %s", strings.TrimSpace(candidateStr))
			wb.pendingCandidates = append(wb.pendingCandidates, ice)
			return
		}

		core.Logger.Trace().Msgf("Adding ICE candidate: %s", strings.TrimSpace(candidateStr))
		if err := wb.peerConnection.AddICECandidate(ice); err != nil {
			core.Logger.Warn().Err(err).Msg("Failed to add ICE candidate")
		}
	}

	wb.cameraClient.HandleError = func(err error) {
		wb.handleError(err)
	}

	wb.cameraClient.HandleDisconnect = func() {
		wb.handleError(errors.New("camera client disconnected"))
	}
}

func (wb *WebRTCBridge) createAndSendOffer() error {
	wb.peerConnection.OnICECandidate(func(candidate *pion.ICECandidate) {
		if candidate != nil {
			core.Logger.Trace().Msgf("Generated ICE candidate: %s", candidate.ToJSON().Candidate)

			if err := wb.cameraClient.SendCandidate("a=" + candidate.ToJSON().Candidate); err != nil {
				core.Logger.Error().Err(err).Msg("Error sending ICE candidate")
			}
		}
	})

	audioDirection := utils.DirectionRecvonly
	if wb.Talkback {
		audioDirection = utils.DirectionSendRecv
	}
	medias := []*utils.Media{
		{Kind: utils.KindAudio, Direction: audioDirection},
		{Kind: utils.KindVideo, Direction: utils.DirectionRecvonly},
	}

	// Create offer
	offer, err := webrtc.CreateOffer(wb.peerConnection, medias)
	if err != nil {
		return fmt.Errorf("failed to create offer: %v", err)
	}

	// Remove extmap lines to reduce payload size (device limitation)
	re := regexp.MustCompile(`\r\na=extmap[^\r\n]*`)
	offer = re.ReplaceAllString(offer, "")

	core.Logger.Trace().Msgf("Sending WebRTC offer")

	// Send offer
	if err := wb.cameraClient.SendOffer(offer, wb.resolution, wb.streamType, wb.isHEVC); err != nil {
		return fmt.Errorf("failed to send offer: %v", err)
	}

	return nil
}

func (wb *WebRTCBridge) handleVideoTrack(track *pion.TrackRemote) {
	core.Logger.Trace().Msgf("Starting video track handler")

	for {
		select {
		case <-wb.ctx.Done():
			return
		default:
			packet, _, err := track.ReadRTP()
			if err != nil {
				if err == io.EOF {
					return
				}
				// Check if it's a known close error
				if strings.Contains(err.Error(), "closed") ||
					strings.Contains(err.Error(), "EOF") ||
					strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				core.Logger.Warn().Err(err).Msg("Unexpected error reading video RTP packet")
				time.Sleep(10 * time.Millisecond)
				continue
			}

			wb.rtpForwarder.ForwardVideoPacket(packet)
		}
	}
}

func (wb *WebRTCBridge) handleAudioTrack(track *pion.TrackRemote) {
	core.Logger.Trace().Msgf("Starting audio track handler")

	for {
		select {
		case <-wb.ctx.Done():
			return
		default:
			packet, _, err := track.ReadRTP()
			if err != nil {
				if err == io.EOF {
					return
				}
				if strings.Contains(err.Error(), "closed") ||
					strings.Contains(err.Error(), "EOF") ||
					strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				core.Logger.Warn().Err(err).Msg("Unexpected error reading audio RTP packet")
				time.Sleep(10 * time.Millisecond)
				continue
			}

			wb.rtpForwarder.ForwardAudioPacket(packet)
		}
	}
}

// createHTTPClient restores legacy session cookies, or returns nil without login state.
func (wb *WebRTCBridge) createHTTPClient() *http.Client {
	if wb.user == nil || wb.user.SessionData == nil {
		return nil
	}
	jar, err := cookiejar.New(&cookiejar.Options{
		PublicSuffixList: publicsuffix.List,
	})
	if err != nil {
		return nil
	}

	if wb.user.SessionData != nil && len(wb.user.SessionData.Cookies) > 0 {
		serverURL, _ := url.Parse(fmt.Sprintf("https://%s", wb.user.SessionData.ServerHost))

		var httpCookies []*http.Cookie
		for _, cookie := range wb.user.SessionData.Cookies {
			httpCookies = append(httpCookies, &http.Cookie{
				Name:     cookie.Name,
				Value:    cookie.Value,
				Domain:   cookie.Domain,
				Path:     cookie.Path,
				Expires:  cookie.Expires,
				Secure:   cookie.Secure,
				HttpOnly: cookie.HttpOnly,
			})
		}

		jar.SetCookies(serverURL, httpCookies)
	}

	return &http.Client{
		Timeout: 30 * time.Second,
		Jar:     jar,
	}
}

func (wb *WebRTCBridge) probe(msg pion.DataChannelMessage) (bool, error) {
	var message tuya.DataChannelMessage
	if err := json.Unmarshal([]byte(msg.Data), &message); err != nil {
		return false, err
	}

	switch message.Type {
	case "codec":
		frameRequest, _ := json.Marshal(tuya.DataChannelMessage{
			Type: "start",
			Msg:  "frame",
		})

		err := wb.sendMessageToDataChannel(frameRequest)
		if err != nil {
			return false, err
		}

	case "recv":
		var recvMessage tuya.RecvMessage
		if err := json.Unmarshal([]byte(message.Msg), &recvMessage); err != nil {
			return false, err
		}

		wb.rtpForwarder.videoSSRC = recvMessage.Video.SSRC
		wb.rtpForwarder.audioSSRC = recvMessage.Audio.SSRC

		completeMsg, _ := json.Marshal(tuya.DataChannelMessage{
			Type: "complete",
			Msg:  "",
		})

		err := wb.sendMessageToDataChannel(completeMsg)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	return false, nil
}

func (wb *WebRTCBridge) sendMessageToDataChannel(message []byte) error {
	if wb.dataChannel != nil {
		return wb.dataChannel.Send(message)
	}

	return nil
}

// SetErrorHandler safely replaces the stream owner's asynchronous failure callback.
func (wb *WebRTCBridge) SetErrorHandler(handler func(error)) {
	wb.callbackMutex.Lock()
	defer wb.callbackMutex.Unlock()
	wb.OnError = handler
}

// handleError completes negotiation and notifies the owner without blocking signaling.
func (wb *WebRTCBridge) handleError(err error) {
	// Always finish negotiation, including standalone bridges without a callback.
	wb.waiter.Done(err)
	wb.connected.Store(false)
	if wb.ctx.Err() != nil {
		return // Expected close callbacks during intentional teardown.
	}
	wb.cancel()
	wb.callbackMutex.RLock()
	handler := wb.OnError
	wb.callbackMutex.RUnlock()
	if handler != nil {
		// Never block MQTT/Pion dispatch on stream locks or a disconnect PUBACK.
		go handler(err)
	}
}
