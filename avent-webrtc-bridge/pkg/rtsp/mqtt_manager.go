package rtsp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"avent-webrtc-bridge/pkg/core"
	"avent-webrtc-bridge/pkg/tuya"
)

var mqttBackoffDelays = []time.Duration{2 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second}

type MQTTManager struct {
	mobileClient *tuya.MobileSDKClient
	clients      map[string]*tuya.MQTTClient // keyed by deviceId
	pending      map[string]*mqttConnectCall
	mutex        sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	connectFn    func(context.Context, string) (*tuya.MQTTClient, error)
}

type mqttConnectCall struct {
	done   chan struct{}
	cancel context.CancelFunc
	client *tuya.MQTTClient
	err    error
}

// NewMQTTManager ties cached MQTT sessions and in-flight connects to the server lifetime.
func NewMQTTManager(parent context.Context, mobileClient *tuya.MobileSDKClient) *MQTTManager {
	ctx, cancel := context.WithCancel(parent)
	m := &MQTTManager{
		mobileClient: mobileClient,
		clients:      make(map[string]*tuya.MQTTClient),
		pending:      make(map[string]*mqttConnectCall),
		ctx:          ctx,
		cancel:       cancel,
	}
	m.connectFn = m.connect
	return m
}

// GetClient shares one in-flight connection per device without locking network waits.
// Canceling the initiating caller cancels that attempt; later calls may retry.
func (m *MQTTManager) GetClient(ctx context.Context, deviceId string) (*tuya.MQTTClient, error) {
	m.mutex.Lock()
	if err := ctx.Err(); err != nil {
		m.mutex.Unlock()
		return nil, err
	}
	if err := m.ctx.Err(); err != nil {
		m.mutex.Unlock()
		return nil, err
	}
	if client, exists := m.clients[deviceId]; exists && client.IsConnected() {
		m.mutex.Unlock()
		core.Logger.Trace().Msgf("Reusing existing MQTT client for device %s", deviceId)
		return client, nil
	}
	if call := m.pending[deviceId]; call != nil {
		m.mutex.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-m.ctx.Done():
			return nil, m.ctx.Err()
		case <-call.done:
			return call.client, call.err
		}
	}
	stale := m.clients[deviceId]
	delete(m.clients, deviceId)
	connectCtx, cancel := context.WithCancel(m.ctx)
	call := &mqttConnectCall{done: make(chan struct{}), cancel: cancel}
	m.pending[deviceId] = call
	m.mutex.Unlock()
	stopCancel := context.AfterFunc(ctx, cancel)
	defer stopCancel()
	defer cancel()

	if stale != nil {
		stale.Stop()
	}
	core.Logger.Info().Msgf("Creating new MQTT client for device %s", deviceId)
	client, err := m.connectFn(connectCtx, deviceId)
	m.mutex.Lock()
	if m.pending[deviceId] != call {
		err = context.Canceled
	} else if ctx.Err() != nil {
		err = ctx.Err()
	} else if connectCtx.Err() != nil {
		err = connectCtx.Err()
	}
	if m.pending[deviceId] == call {
		delete(m.pending, deviceId)
	}
	if err == nil {
		m.clients[deviceId] = client
		call.client = client
	}
	call.err = err
	close(call.done)
	m.mutex.Unlock()
	if err != nil && client != nil {
		client.Stop()
	}
	return call.client, err
}

// connect discovers the broker and retries MQTT handshakes with cancellable bounded waits.
func (m *MQTTManager) connect(ctx context.Context, deviceId string) (*tuya.MQTTClient, error) {
	if m.mobileClient == nil {
		return nil, fmt.Errorf("mobile client not configured")
	}

	userInfo, err := m.mobileClient.GetUserInfoContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get user info: %v", err)
	}

	ecode := m.mobileClient.Ecode
	uid := m.mobileClient.UID
	mqttDerived := m.mobileClient.DeriveMQTTConfig(ecode)
	mqttUsername := m.mobileClient.DeriveMQTTUsername(m.mobileClient.SID, ecode, m.mobileClient.PartnerIdentity)
	mqttClientID := m.mobileClient.DeriveMQTTClientID(uid)
	subscribeTopic := fmt.Sprintf("/av/u/%s", uid)
	brokerURL := fmt.Sprintf("ssl://%s:8883", userInfo.Domain.MobileMqttsUrl)

	var lastErr error
	for attempt, delay := range mqttBackoffDelays {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if attempt > 0 {
			core.Logger.Info().Msgf("MQTT connect retry %d/%d for device %s in %v", attempt+1, len(mqttBackoffDelays), deviceId, delay)
			if err := waitMQTTBackoff(ctx, delay); err != nil {
				return nil, err
			}
		}

		client, err := tuya.NewMobileMqttClientContext(ctx, &tuya.MobileMQTTConfig{
			Username:       mqttUsername,
			Password:       mqttDerived.Password,
			ClientID:       mqttClientID,
			SubscribeTopic: subscribeTopic,
			UID:            uid,
			BrokerURL:      brokerURL,
		})
		if err != nil {
			lastErr = err
			core.Logger.Warn().Err(err).Msgf("MQTT connect attempt %d/%d failed for device %s", attempt+1, len(mqttBackoffDelays), deviceId)
			continue
		}

		if err = client.WaitConnected(ctx); err != nil {
			lastErr = err
			client.Stop()
			core.Logger.Warn().Err(err).Msgf("MQTT handshake attempt %d/%d failed for device %s", attempt+1, len(mqttBackoffDelays), deviceId)
			continue
		}

		core.Logger.Info().Msgf("MQTT connected for device %s", deviceId)
		return client, nil
	}

	return nil, fmt.Errorf("MQTT connect failed after %d attempts: %v", len(mqttBackoffDelays), lastErr)
}

// waitMQTTBackoff stops retry delays immediately when the server or caller stops.
func waitMQTTBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// InvalidateClient removes a cached session and cancels an obsolete in-flight attempt.
func (m *MQTTManager) InvalidateClient(deviceId string) {
	m.mutex.Lock()
	client := m.clients[deviceId]
	delete(m.clients, deviceId)
	if call := m.pending[deviceId]; call != nil {
		call.cancel()
		delete(m.pending, deviceId)
	}
	m.mutex.Unlock()
	if client != nil {
		core.Logger.Info().Msgf("Invalidating MQTT client for device %s (will reconnect on next use)", deviceId)
		client.Stop()
	}
}

// Stop cancels in-flight work before closing cached clients outside the manager lock.
func (m *MQTTManager) Stop() {
	m.cancel()
	m.mutex.Lock()
	clients := m.clients
	m.clients = make(map[string]*tuya.MQTTClient)
	m.mutex.Unlock()
	for deviceId, client := range clients {
		core.Logger.Info().Msgf("Closing MQTT client for device %s", deviceId)
		client.Stop()
	}
}
