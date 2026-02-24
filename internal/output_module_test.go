package internal

import (
	"context"
	"sync"
	"testing"
	"time"
)

// mockMessageSubscriber captures subscriptions and allows simulating message delivery.
type mockMessageSubscriber struct {
	mu            sync.Mutex
	subscriptions map[string]func(payload []byte, metadata map[string]string) error
}

func newMockMessageSubscriber() *mockMessageSubscriber {
	return &mockMessageSubscriber{
		subscriptions: make(map[string]func(payload []byte, metadata map[string]string) error),
	}
}

func (m *mockMessageSubscriber) Subscribe(topic string, handler func(payload []byte, metadata map[string]string) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscriptions[topic] = handler
	return nil
}

func (m *mockMessageSubscriber) Unsubscribe(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.subscriptions, topic)
	return nil
}

// SimulateMessage simulates delivering a message to a subscribed topic.
func (m *mockMessageSubscriber) SimulateMessage(topic string, payload []byte, metadata map[string]string) error {
	m.mu.Lock()
	handler, ok := m.subscriptions[topic]
	m.mu.Unlock()

	if !ok {
		return nil
	}
	return handler(payload, metadata)
}

func TestNewOutputModule(t *testing.T) {
	tests := []struct {
		name    string
		modName string
		config  map[string]any
		wantErr bool
	}{
		{
			name:    "valid config",
			modName: "test-output",
			config: map[string]any{
				"source_topic": "test-topic",
				"output": map[string]any{
					"drop": map[string]any{},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := newOutputModule(tt.modName, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("newOutputModule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && m.name != tt.modName {
				t.Errorf("expected name %q, got %q", tt.modName, m.name)
			}
		})
	}
}

func TestOutputModule_Init(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]any
		wantErr bool
	}{
		{
			name: "valid config with source_topic",
			config: map[string]any{
				"source_topic": "my-topic",
				"output": map[string]any{
					"drop": map[string]any{},
				},
			},
			wantErr: false,
		},
		{
			name: "missing source_topic",
			config: map[string]any{
				"output": map[string]any{
					"drop": map[string]any{},
				},
			},
			wantErr: true,
		},
		{
			name: "empty source_topic",
			config: map[string]any{
				"source_topic": "",
				"output": map[string]any{
					"drop": map[string]any{},
				},
			},
			wantErr: true,
		},
		{
			name: "missing output config",
			config: map[string]any{
				"source_topic": "my-topic",
			},
			wantErr: true,
		},
		{
			name: "with source_broker",
			config: map[string]any{
				"source_topic":  "my-topic",
				"source_broker": "my-broker",
				"output": map[string]any{
					"drop": map[string]any{},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newOutputModule("test", tt.config)
			err := m.Init()
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOutputModule_SetMessageSubscriber(t *testing.T) {
	m, _ := newOutputModule("test", map[string]any{})
	sub := newMockMessageSubscriber()

	m.SetMessageSubscriber(sub)

	// Just verify it doesn't panic and is stored internally
	// Can't directly compare interface values
}

func TestOutputModule_SetMessagePublisher(t *testing.T) {
	m, _ := newOutputModule("test", map[string]any{})
	// Should not panic - outputModule doesn't use publisher
	m.SetMessagePublisher(nil)
}

func TestOutputModule_StartWithoutSubscriber(t *testing.T) {
	m, _ := newOutputModule("test", map[string]any{
		"source_topic": "test-topic",
		"output": map[string]any{
			"drop": map[string]any{},
		},
	})

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	// Start without subscriber should error
	ctx := context.Background()
	err := m.Start(ctx)
	if err == nil {
		t.Error("Start() without subscriber should error")
		_ = m.Stop(context.Background())
	}
}

func TestOutputModule_SubscribeAndReceiveMessages(t *testing.T) {
	sub := newMockMessageSubscriber()

	m, _ := newOutputModule("test-output", map[string]any{
		"source_topic": "test-topic",
		"output": map[string]any{
			"drop": map[string]any{},
		},
	})

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	m.SetMessageSubscriber(sub)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Verify subscription was registered
	sub.mu.Lock()
	_, subscribed := sub.subscriptions["test-topic"]
	sub.mu.Unlock()

	if !subscribed {
		t.Error("expected subscription to test-topic")
	}

	// Simulate sending messages
	testPayload := []byte(`{"test": "data"}`)
	testMetadata := map[string]string{"source": "test"}

	if err := sub.SimulateMessage("test-topic", testPayload, testMetadata); err != nil {
		t.Errorf("SimulateMessage() error = %v", err)
	}

	// Wait until the module has processed the message rather than sleeping.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if m.metrics.Snapshot().MessagesIn >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("message was not processed within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := m.Stop(stopCtx); err != nil {
		t.Errorf("Stop() error = %v", err)
	}

	// Verify unsubscribe happened
	sub.mu.Lock()
	_, stillSubscribed := sub.subscriptions["test-topic"]
	sub.mu.Unlock()

	if stillSubscribed {
		t.Error("expected unsubscribe from test-topic after Stop")
	}
}

func TestOutputModule_Health(t *testing.T) {
	sub := newMockMessageSubscriber()

	m, _ := newOutputModule("test-output", map[string]any{
		"source_topic": "test-topic",
		"output": map[string]any{
			"drop": map[string]any{},
		},
	})

	// Before start: unhealthy
	report := m.Health()
	if report.Status != HealthStatusUnhealthy {
		t.Errorf("expected unhealthy before start, got %s", report.StatusText)
	}

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	m.SetMessageSubscriber(sub)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Wait until the module becomes healthy or time out.
	deadline := time.Now().Add(2 * time.Second)
	for {
		report = m.Health()
		if report.Status == HealthStatusHealthy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("module did not become healthy within timeout, last status: %s", report.StatusText)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// After start: healthy
	report = m.Health()
	if report.Status != HealthStatusHealthy {
		t.Errorf("expected healthy after start, got %s", report.StatusText)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := m.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	// After stop: unhealthy
	report = m.Health()
	if report.Status != HealthStatusUnhealthy {
		t.Errorf("expected unhealthy after stop, got %s", report.StatusText)
	}
}

func TestOutputModule_InvalidOutputConfig(t *testing.T) {
	m, _ := newOutputModule("test", map[string]any{
		"source_topic": "test-topic",
		"output":       "not-a-map", // invalid: should be map
	})

	if err := m.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	sub := newMockMessageSubscriber()
	m.SetMessageSubscriber(sub)

	ctx := context.Background()
	// Start should fail with invalid output config type
	err := m.Start(ctx)
	if err == nil {
		t.Error("Start() expected error for invalid output config type")
		_ = m.Stop(context.Background())
	}
}
