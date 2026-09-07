package tuya

import (
	"avent-webrtc-bridge/pkg/core"
	"avent-webrtc-bridge/pkg/utils"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MQTTClient struct {
	mqtt           mqtt.Client
	uid            string
	subscribeTopic string
	cameras        map[string]*MQTTCameraClient // sessionId -> camera
	closed         atomic.Bool
	stopOnce       sync.Once
	camerasMutex   sync.RWMutex
	Connected      utils.Waiter
	stopCtx        context.Context
	cancel         context.CancelFunc
}

type MqttFrameHeader struct {
	Type          string `json:"type"`
	From          string `json:"from"`
	To            string `json:"to"`
	SubDevID      string `json:"sub_dev_id"`
	SessionID     string `json:"sessionid"`
	MotoID        string `json:"moto_id"`
	TransactionID string `json:"tid"`
	Seq           int    `json:"seq"`
	Rtx           int    `json:"rtx"`
}

type MqttFrame struct {
	Header  MqttFrameHeader `json:"header"`
	Message json.RawMessage `json:"msg"`
}

type MqttMessage struct {
	Protocol int       `json:"protocol"`
	Pv       string    `json:"pv"`
	T        int64     `json:"t"`
	Data     MqttFrame `json:"data"`
}

// NewMqttClient opens a web MQTT session with bounded default handshake waits.
func NewMqttClient(clientId, mobileMqttsUrl string, mqttConfig *MQTConfig) (*MQTTClient, error) {
	return NewMqttClientContext(context.Background(), clientId, mobileMqttsUrl, mqttConfig)
}

// NewMqttClientContext permits cancellation while the web MQTT connection is opening.
func NewMqttClientContext(ctx context.Context, clientId, mobileMqttsUrl string, mqttConfig *MQTConfig) (*MQTTClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stopCtx, cancel := context.WithCancel(context.Background())
	client := &MQTTClient{
		uid:            mqttConfig.Msid,
		subscribeTopic: fmt.Sprintf("/av/u/%s", mqttConfig.Msid),
		Connected:      utils.Waiter{},
		stopCtx:        stopCtx,
		cancel:         cancel,
	}

	wssUrl := fmt.Sprintf("wss://%s/mqtt", mobileMqttsUrl)
	username := fmt.Sprintf("web_%s", mqttConfig.Msid)
	password := mqttConfig.Password

	opts := mqtt.NewClientOptions()
	opts.AddBroker(wssUrl)
	opts.SetClientID(username)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetOnConnectHandler(client.onConnect)
	opts.SetConnectionLostHandler(client.onDisconnect)
	opts.SetAutoReconnect(false)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetKeepAlive(60 * time.Second)
	opts.SetCleanSession(true)

	client.mqtt = mqtt.NewClient(opts)
	if err := waitMQTTTokenContext(ctx, client.mqtt.Connect(), 15*time.Second); err != nil {
		client.Stop()
		return nil, err
	}

	return client, nil
}

type MobileMQTTConfig struct {
	Username       string
	Password       string
	ClientID       string
	SubscribeTopic string
	UID            string
	BrokerURL      string
}

// NewMobileMqttClient opens a mobile MQTT session with bounded default waits.
func NewMobileMqttClient(config *MobileMQTTConfig) (*MQTTClient, error) {
	return NewMobileMqttClientContext(context.Background(), config)
}

// NewMobileMqttClientContext cancels the connect-token wait with its caller.
func NewMobileMqttClientContext(ctx context.Context, config *MobileMQTTConfig) (*MQTTClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stopCtx, cancel := context.WithCancel(context.Background())
	client := &MQTTClient{
		uid:            config.UID,
		subscribeTopic: config.SubscribeTopic,
		Connected:      utils.Waiter{},
		stopCtx:        stopCtx,
		cancel:         cancel,
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.BrokerURL)
	opts.SetClientID(config.ClientID)
	opts.SetUsername(config.Username)
	opts.SetPassword(config.Password)
	opts.SetOnConnectHandler(client.onConnect)
	opts.SetConnectionLostHandler(client.onDisconnect)
	opts.SetAutoReconnect(false)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetKeepAlive(60 * time.Second)
	opts.SetCleanSession(true)

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	opts.SetTLSConfig(tlsConfig)

	client.mqtt = mqtt.NewClient(opts)
	if err := waitMQTTTokenContext(ctx, client.mqtt.Connect(), 15*time.Second); err != nil {
		client.Stop()
		return nil, err
	}

	return client, nil
}

// Stop cancels subscription waiters and disconnects this MQTT session at most once.
func (c *MQTTClient) Stop() {
	c.stopOnce.Do(func() {
		c.closed.Store(true)
		if c.cancel != nil {
			c.cancel()
		}
		c.Connected.Done(context.Canceled)
		if c.mqtt != nil {
			c.mqtt.Disconnect(250)
		}
	})
}

// IsConnected combines transport readiness with the session's terminal closed state.
func (c *MQTTClient) IsConnected() bool {
	return c.mqtt != nil && c.mqtt.IsConnected() && !c.closed.Load()
}

// AddCameraClient safely registers a signaling recipient for a camera session.
func (c *MQTTClient) AddCameraClient(sessionId string, cameraClient *MQTTCameraClient) {
	c.camerasMutex.Lock()
	defer c.camerasMutex.Unlock()
	if c.cameras == nil {
		c.cameras = make(map[string]*MQTTCameraClient)
	}
	c.cameras[sessionId] = cameraClient
}

// RemoveCameraClient safely unregisters a camera during signaling or teardown.
func (c *MQTTClient) RemoveCameraClient(sessionId string) {
	c.camerasMutex.Lock()
	defer c.camerasMutex.Unlock()
	if c.cameras == nil {
		return
	}
	delete(c.cameras, sessionId)
}

// onConnect completes readiness only after a bounded, stoppable subscription handshake.
func (c *MQTTClient) onConnect(client mqtt.Client) {
	core.Logger.Trace().Msgf("Connected to mqtt broker")
	core.Logger.Trace().Msgf("Subscribing to topic: %s", c.subscribeTopic)

	if err := waitMQTTTokenContext(c.stopCtx, client.Subscribe(c.subscribeTopic, 1, c.consume), 10*time.Second); err != nil {
		c.Connected.Done(err)
		return
	}

	core.Logger.Trace().Msgf("Subscribed")
	c.Connected.Done(nil)
}

// onDisconnect publishes handshake failure and marks the session permanently closed.
func (c *MQTTClient) onDisconnect(client mqtt.Client, err error) {
	if err != nil {
		c.Connected.Done(err)
	} else {
		c.Connected.Done(errors.New("mqtt client disconnected"))
	}

	c.closed.Store(true)
}

// consume snapshots a registered recipient before dispatching camera signaling without a map lock.
func (c *MQTTClient) consume(client mqtt.Client, msg mqtt.Message) {
	var rmqtt MqttMessage
	if err := json.Unmarshal(msg.Payload(), &rmqtt); err != nil {
		core.Logger.Error().Err(err).Msg("Failed to unmarshal mqtt message: %s")
		return
	}

	sessionId := rmqtt.Data.Header.SessionID
	c.camerasMutex.RLock()
	cameraClient, ok := c.cameras[sessionId]
	c.camerasMutex.RUnlock()
	if !ok {
		core.Logger.Warn().Msgf("No camera client found for sessionId: %s", sessionId)
		return
	}

	switch rmqtt.Data.Header.Type {
	case "answer":
		cameraClient.onMqttAnswer(&rmqtt)
	case "candidate":
		cameraClient.onMqttCandidate(&rmqtt)
	case "disconnect":
		cameraClient.onMqttDisconnect()
	}
}

// waitMQTTToken bounds acknowledgements for legacy callers without a context.
func waitMQTTToken(token mqtt.Token, timeout time.Duration) error {
	return waitMQTTTokenContext(context.Background(), token, timeout)
}

// waitMQTTTokenContext waits directly on token completion without spawning waiters.
func waitMQTTTokenContext(parent context.Context, token mqtt.Token, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-token.Done():
		return token.Error()
	}
}

// WaitConnected bounds subscription readiness by both caller cancellation and a handshake deadline.
func (c *MQTTClient) WaitConnected(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	return c.Connected.WaitContext(ctx)
}
